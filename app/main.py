from contextlib import asynccontextmanager
import logging
import secrets
from pathlib import Path

import httpx
from fastapi import (
    Cookie,
    Depends,
    Header,
    FastAPI,
    HTTPException,
    Request,
    Response,
    status,
)
from fastapi.responses import FileResponse, JSONResponse

from app.config import settings
from app.models import (
    ChatHistoryResponse,
    ChatMessage,
    ChatRequest,
    ChatResponse,
    ChatSessionSummary,
    LoginRequest,
    LoginResponse,
)
from app.services.auth import auth_service
from app.services.database import db
from app.services.databricks_auth import (
    DatabricksAuthError,
    databricks_auth,
)
from app.services.genie_client import (
    GenieError,
    genie_client,
)
from app.services.rate_limiter import (
    RateLimitError,
    chat_limiter,
    followup_limiter,
    genie_semaphore,
    login_limiter,
)
from app.services.session_store import (
    SessionStoreError,
    session_store,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("app")


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


# Databricks has returned both legacy top-level IDs and the newer
# nested conversation/message response shape. Support both.
def extract_genie_ids(payload: dict) -> tuple[str | None, str | None]:
    conversation_id = payload.get("conversation_id")
    message_id = payload.get("message_id")

    conversation = payload.get("conversation") or {}
    message = payload.get("message") or {}

    conversation_id = (
        conversation_id
        or conversation.get("id")
        or conversation.get("conversation_id")
        or message.get("conversation_id")
    )
    message_id = (
        message_id
        or message.get("message_id")
        or message.get("id")
    )

    return conversation_id, message_id


# =========================================================
# APPLICATION LIFECYCLE
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    await db.connect()

    await auth_service.initialize()

    await session_store.initialize()

    try:
        yield
    finally:
        await genie_client.close()
        close_auth = getattr(databricks_auth, "close", None)
        if close_auth is not None:
            result = close_auth()
            if hasattr(result, "__await__"):
                await result
        await db.close()


app = FastAPI(
    title="Databricks Genie Chatbot",
    version="1.0.0",
    lifespan=lifespan,
)


# =========================================================
# GLOBAL ERROR HANDLING
# =========================================================
#
# Any unexpected exception used to produce a plain-text
# "Internal Server Error" body. The frontend can only show a
# message when the body is JSON with a "detail" field, so it fell
# back to the generic "Request failed." text.

@app.exception_handler(httpx.HTTPError)
async def httpx_exception_handler(
    request: Request,
    exc: httpx.HTTPError,
):

    logger.exception(
        "Upstream HTTP error on %s %s",
        request.method,
        request.url.path,
    )

    return JSONResponse(
        status_code=502,
        content={
            "detail": (
                "Connection to Databricks failed "
                f"({type(exc).__name__}). Please try again."
            )
        },
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(
    request: Request,
    exc: Exception,
):

    logger.exception(
        "Unhandled error on %s %s",
        request.method,
        request.url.path,
    )

    return JSONResponse(
        status_code=500,
        content={
            "detail": (
                f"Internal server error ({type(exc).__name__}). "
                "Check the server log for details."
            )
        },
    )


# =========================================================
# AUTH DEPENDENCY
# =========================================================

async def get_current_user(
    app_session: str | None = Cookie(
        default=None,
        alias="app_session",
    ),
) -> str:

    if not app_session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated.",
        )

    username = await auth_service.validate_session(
        app_session
    )

    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired or invalid.",
        )

    return username


# =========================================================
# CSRF PROTECTION
# =========================================================

CSRF_COOKIE_NAME = "csrf_token"
CSRF_HEADER_NAME = "X-CSRF-Token"


def _csrf_token_is_valid(
    cookie_token: str | None,
    header_token: str | None,
) -> bool:

    if not cookie_token or not header_token:
        return False

    return secrets.compare_digest(
        cookie_token,
        header_token,
    )


async def require_csrf(
    csrf_cookie: str | None = Cookie(
        default=None,
        alias=CSRF_COOKIE_NAME,
    ),
    csrf_header: str | None = Header(
        default=None,
        alias=CSRF_HEADER_NAME,
    ),
) -> None:

    if not _csrf_token_is_valid(
        csrf_cookie,
        csrf_header,
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF validation failed.",
        )


# =========================================================
# FRONTEND
# =========================================================

@app.get(
    "/",
    include_in_schema=False,
)
async def index():

    return FileResponse(
        STATIC_DIR / "index.html"
    )


# =========================================================
# HEALTH
# =========================================================

@app.get("/health")
async def health():

    try:

        await db.fetch_one(
            "SELECT 1 AS health"
        )

        return {
            "status": "ok",
            "database": "postgresql",
        }

    except Exception:

        raise HTTPException(
            status_code=503,
            detail="Database unavailable.",
        )


# =========================================================
# CSRF TOKEN
# =========================================================

@app.get("/api/csrf")
async def csrf_token(
    response: Response,
    csrf_cookie: str | None = Cookie(
        default=None,
        alias=CSRF_COOKIE_NAME,
    ),
):

    token = csrf_cookie or secrets.token_urlsafe(32)

    response.set_cookie(
        key=CSRF_COOKIE_NAME,
        value=token,
        httponly=False,
        secure=settings.app_cookie_secure,
        samesite=settings.app_cookie_samesite,
        path="/",
    )

    return {"csrf_token": token}


# =========================================================
# LOGIN
# =========================================================

@app.post(
    "/api/login",
    response_model=LoginResponse,
)
async def login(
    request: LoginRequest,
    response: Response,
    _: None = Depends(require_csrf),
):

    try:

        login_limiter.check(
            request.username
        )

    except RateLimitError as exc:

        raise HTTPException(
            status_code=429,
            detail=(
                f"Too many login attempts. "
                f"Try again in {exc.retry_after} seconds."
            ),
            headers={
                "Retry-After": str(
                    exc.retry_after
                )
            },
        )

    if not auth_service.authenticate(
        request.username,
        request.password,
    ):

        raise HTTPException(
            status_code=401,
            detail="Invalid username or password.",
        )

    token = await auth_service.create_session(
        request.username
    )

    response.set_cookie(
        key="app_session",
        value=token,
        max_age=(
            settings.app_auth_session_ttl_hours
            * 60
            * 60
        ),
        httponly=True,
        secure=settings.app_cookie_secure,
        samesite=settings.app_cookie_samesite,
        path="/",
    )

    return LoginResponse(
        status="ok"
    )


# =========================================================
# LOGOUT
# =========================================================

@app.post(
    "/api/logout",
)
async def logout(
    response: Response,
    app_session: str | None = Cookie(
        default=None,
        alias="app_session",
    ),
    _: None = Depends(require_csrf),
):

    if app_session:

        await auth_service.delete_session(
            app_session
        )

    response.delete_cookie(
        key="app_session",
        path="/",
    )

    return {
        "status": "ok"
    }


# =========================================================
# CURRENT SESSION
# =========================================================

@app.get("/api/session")
async def current_session(
    username: str = Depends(
        get_current_user
    ),
):

    return {
        "authenticated": True,
        "username": username,
    }


# =========================================================
# LIST CHAT SESSIONS
# =========================================================

@app.get(
    "/api/chats",
    response_model=list[ChatSessionSummary],
)
async def list_chats(
    username: str = Depends(
        get_current_user
    ),
):

    sessions = await session_store.list_sessions(
        username
    )

    return [
        ChatSessionSummary(
            session_id=session[
                "session_id"
            ],
            title=session["title"],
            created_at=session[
                "created_at"
            ].isoformat(),
            last_activity_at=session[
                "last_activity_at"
            ].isoformat(),
        )
        for session in sessions
    ]


# =========================================================
# GET CHAT HISTORY
# =========================================================

@app.get(
    "/api/chats/{session_id}",
    response_model=ChatHistoryResponse,
)
async def get_chat(
    session_id: str,
    username: str = Depends(
        get_current_user
    ),
):

    history = await session_store.get_chat_history(
        session_id,
        username,
    )

    if history is None:

        raise HTTPException(
            status_code=404,
            detail="Chat session not found.",
        )

    return ChatHistoryResponse(
        session_id=history["session_id"],
        title=history["title"],
        created_at=history["created_at"],
        last_activity_at=history[
            "last_activity_at"
        ],
        messages=[
            ChatMessage(
                role=message["role"],
                content=message["content"],
                created_at=message[
                    "created_at"
                ],
                presentation=message.get(
                    "presentation",
                    {},
                ),
            )
            for message in history["messages"]
        ],
    )


# =========================================================
# GENIE VISUALIZATION IMAGE
# =========================================================

@app.get(
    "/api/genie/visualizations/{conversation_id}/{message_id}/{attachment_id}",
)
async def genie_visualization(
    conversation_id: str,
    message_id: str,
    attachment_id: str,
    username: str = Depends(get_current_user),
):
    # Confirm that the conversation belongs to the authenticated user.
    owned_session = await session_store.find_session_by_genie_conversation(
        conversation_id,
        username,
    )

    if not owned_session:
        raise HTTPException(
            status_code=404,
            detail="Visualization not found.",
        )

    try:
        image = await genie_client.download_visualization(
            conversation_id,
            message_id,
            attachment_id,
        )
    except (GenieError, DatabricksAuthError) as exc:
        raise HTTPException(
            status_code=502,
            detail=str(exc),
        )

    return Response(
        content=image,
        media_type="image/png",
        headers={"Cache-Control": "private, max-age=300"},
    )


# =========================================================
# FULL GENIE QUERY-RESULT DOWNLOAD
# =========================================================

@app.get(
    "/api/genie/query-results/{conversation_id}/{message_id}/{attachment_id}/download",
)
async def genie_query_result_download(
    conversation_id: str,
    message_id: str,
    attachment_id: str,
    username: str = Depends(get_current_user),
):
    owned_session = await session_store.find_session_by_genie_conversation(
        conversation_id,
        username,
    )

    if not owned_session:
        raise HTTPException(
            status_code=404,
            detail="Query result not found.",
        )

    try:
        links = await genie_client.get_full_query_download_links(
            conversation_id,
            message_id,
            attachment_id,
        )
    except (GenieError, DatabricksAuthError) as exc:
        raise HTTPException(
            status_code=502,
            detail=str(exc),
        )

    # These are short-lived Databricks signed URLs. They are returned only
    # to the already-authenticated browser and are never stored server-side.
    return {"download_urls": links}


# =========================================================
# DELETE CHAT
# =========================================================

@app.delete(
    "/api/chats/{session_id}",
)
async def delete_chat(
    session_id: str,
    username: str = Depends(
        get_current_user
    ),
    _: None = Depends(require_csrf),
):

    await session_store.delete_session(
        session_id,
        username,
    )

    return {
        "status": "ok"
    }


# =========================================================
# GENIE AGENT TURN (shared by new chat and follow-up)
# =========================================================

async def run_agent_turn(
    message: str,
    conversation_id: str | None,
) -> dict:
    """Run one Genie Agent response and build everything the API returns.

    Streaming interruptions are retried inside genie_client. Sessions that
    were created before the Agent-mode migration point at a legacy
    Chat-mode conversation, which Databricks rejects on the Agent endpoint;
    for those a fresh Agent conversation is started transparently.

    Returns a dict with: agent_response, conversation_id, conversation_changed,
    answer, presentation, message_id.
    """

    original_conversation_id = conversation_id
    rebound = False

    async with genie_semaphore:

        try:
            agent_response = (
                await genie_client.create_agent_response(
                    message,
                    conversation_id=conversation_id,
                    enable_visualization=True,
                )
            )
        except GenieError as exc:
            if (
                conversation_id
                and genie_client.is_legacy_conversation_error(exc)
            ):
                rebound = True
                agent_response = (
                    await genie_client.create_agent_response(
                        message,
                        conversation_id=None,
                        enable_visualization=True,
                    )
                )
            else:
                raise

        returned_conversation_id = agent_response.get(
            "conversation_id"
        )

        # After a legacy rebind the old ID must never be reused.
        resolved_conversation_id = (
            returned_conversation_id
            or (None if rebound else conversation_id)
        )

        if not resolved_conversation_id:
            raise GenieError(
                "Genie Agent did not return a conversation ID."
            )

        answer = genie_client.normalize_answer_text(
            genie_client.extract_agent_answer(
                agent_response
            )
        )

        presentation = await genie_client.build_agent_presentation(
            agent_response
        )

    # Agent mode does not expose the old chat-mode message_id.
    # The response ID is the closest stable response identifier.
    message_id = (
        presentation.get("agent_message_id")
        or agent_response.get("id")
        or ""
    )

    return {
        "agent_response": agent_response,
        "conversation_id": resolved_conversation_id,
        "conversation_changed": (
            resolved_conversation_id != original_conversation_id
        ),
        "answer": answer,
        "presentation": presentation,
        "message_id": message_id,
    }


def _raise_bad_gateway(exc: Exception) -> None:
    logger.warning("Genie request failed: %s", exc)
    raise HTTPException(
        status_code=502,
        detail=str(exc),
    )


# =========================================================
# NEW CHAT
# =========================================================

@app.post(
    "/api/chat",
    response_model=ChatResponse,
)
async def new_chat(
    request: ChatRequest,
    username: str = Depends(
        get_current_user
    ),
    _: None = Depends(require_csrf),
):

    try:

        chat_limiter.check(username)

    except RateLimitError as exc:

        raise HTTPException(
            status_code=429,
            detail=(
                f"Too many requests. "
                f"Try again in {exc.retry_after} seconds."
            ),
            headers={
                "Retry-After": str(
                    exc.retry_after
                )
            },
        )

    try:

        turn = await run_agent_turn(
            request.message,
            conversation_id=None,
        )

    except (
        GenieError,
        DatabricksAuthError,
        httpx.HTTPError,
    ) as exc:

        _raise_bad_gateway(exc)

    title = request.message.strip()

    if len(title) > 60:
        title = title[:57] + "..."

    session_id = await session_store.create_session(
        genie_conversation_id=turn["conversation_id"],
        username=username,
        title=title,
    )

    await session_store.add_message(
        session_id=session_id,
        username=username,
        role="user",
        content=request.message,
    )

    await session_store.add_message(
        session_id=session_id,
        username=username,
        role="assistant",
        content=turn["answer"],
        metadata=turn["presentation"],
    )

    return ChatResponse(
        session_id=session_id,
        message_id=turn["message_id"],
        status=turn["agent_response"].get(
            "status",
            "completed",
        ),
        answer=turn["answer"],
        presentation=turn["presentation"],
    )


# =========================================================
# FOLLOW-UP MESSAGE
# =========================================================

@app.post(
    "/api/chat/{session_id}",
    response_model=ChatResponse,
)
async def followup_chat(
    session_id: str,
    request: ChatRequest,
    username: str = Depends(
        get_current_user
    ),
    _: None = Depends(require_csrf),
):

    try:

        followup_limiter.check(username)

    except RateLimitError as exc:

        raise HTTPException(
            status_code=429,
            detail=(
                f"Too many requests. "
                f"Try again in {exc.retry_after} seconds."
            ),
            headers={
                "Retry-After": str(
                    exc.retry_after
                )
            },
        )

    conversation_id = (
        await session_store.get_genie_conversation_id(
            session_id,
            username,
        )
    )

    if not conversation_id:

        raise HTTPException(
            status_code=404,
            detail="Chat session not found.",
        )

    try:

        turn = await run_agent_turn(
            request.message,
            conversation_id=conversation_id,
        )

    except (
        GenieError,
        DatabricksAuthError,
        httpx.HTTPError,
    ) as exc:

        _raise_bad_gateway(exc)

    if turn["conversation_changed"]:

        # Legacy Chat-mode session rebound to a fresh Agent conversation.
        await session_store.set_genie_conversation_id(
            session_id,
            username,
            turn["conversation_id"],
        )

    await session_store.add_message(
        session_id=session_id,
        username=username,
        role="user",
        content=request.message,
    )

    await session_store.add_message(
        session_id=session_id,
        username=username,
        role="assistant",
        content=turn["answer"],
        metadata=turn["presentation"],
    )

    return ChatResponse(
        session_id=session_id,
        message_id=turn["message_id"],
        status=turn["agent_response"].get(
            "status",
            "completed",
        ),
        answer=turn["answer"],
        presentation=turn["presentation"],
    )