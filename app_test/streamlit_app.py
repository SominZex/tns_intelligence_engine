import asyncio
import hashlib
import logging
from typing import Any, Dict, Optional

import httpx
import pandas as pd
import streamlit as st

from genie_client_streamlit import GenieClient, GenieError
from streamlit_runtime import settings
from streamlit_store import store

st.set_page_config(
    page_title="TNS Retail Intelligence",
    page_icon="https://admin.thenewshop.in/static/media/New%20Logo%20.ad69756dd0621a9db47a.jpg",
    layout="wide",
    initial_sidebar_state="expanded",
)

LOGO_URL = "https://admin.thenewshop.in/static/media/New%20Logo%20.ad69756dd0621a9db47a.jpg"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tns_streamlit")


def inject_css():
    st.markdown(
        """
        <style>
        #MainMenu {visibility:hidden;}
        footer {visibility:hidden;}
        header {background:transparent !important;}
        .stApp {background:#f5f7fb; color:#172033;}
        [data-testid="stSidebar"] {background:#111827; min-width:270px; max-width:270px;}
        [data-testid="stSidebar"] * {color:#d1d5db;}
        [data-testid="stSidebar"] .stButton button {text-align:left; border:1px solid #374151; background:#1f2937; color:white; border-radius:9px;}
        [data-testid="stSidebar"] .stButton button:hover {background:#374151; border-color:#4b5563;}
        .tns-brand {display:flex;align-items:center;gap:10px;padding:4px 0 18px;}
        .tns-brand img {width:38px;height:38px;border-radius:9px;object-fit:contain;background:white;}
        .tns-brand-name {font-weight:650;font-size:16px;color:white;}
        .tns-header {background:white;border-bottom:1px solid #e5e7eb;padding:12px 24px;margin:-1rem -1rem 1rem;}
        .tns-header-title {font-weight:600;font-size:15px;color:#172033;}
        .tns-header-subtitle {font-size:11px;color:#9ca3af;margin-top:2px;}
        .welcome {text-align:center;margin:12vh auto 8vh;}
        .welcome h1 {font-size:30px;letter-spacing:-.5px;color:#172033;margin-bottom:10px;}
        .welcome p {font-size:14px;color:#6b7280;}
        .user-bubble {background:#111827;color:white;border-radius:14px 14px 4px 14px;padding:13px 16px;line-height:1.55;margin:12px 0 18px 18%;}
        .assistant-bubble {background:white;border:1px solid #e5e7eb;border-radius:14px 14px 14px 4px;padding:13px 16px;line-height:1.6;margin:12px 18% 18px 0;}
        .section-label {font-size:11px;font-weight:650;text-transform:uppercase;letter-spacing:.7px;color:#9ca3af;margin:14px 0 7px;}
        .result-card {border:1px solid #e5e7eb;border-radius:12px;background:white;padding:14px;margin:12px 0;}
        .result-title {font-weight:650;color:#172033;margin-bottom:8px;}
        .source-note {font-size:11px;color:#9ca3af;margin-top:7px;}
        .login-wrap {max-width:430px;margin:12vh auto 0;background:white;border:1px solid #e5e7eb;border-radius:16px;padding:34px;box-shadow:0 10px 30px rgba(17,24,39,.06);}
        .login-logo {display:block;width:58px;height:58px;object-fit:contain;margin:0 auto 15px;border-radius:12px;background:white;}
        .login-title{text-align:center;font-size:26px;font-weight:700;color:#172033;}
        .login-sub{text-align:center;color:#6b7280;font-size:13px;margin:7px 0 24px;}
        div[data-testid="stChatMessage"] {background:transparent;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def run_async(coro):
    return asyncio.run(coro)


def new_client() -> GenieClient:
    return GenieClient()


def call_genie(coro_factory):
    client = new_client()
    try:
        return run_async(coro_factory(client))
    finally:
        run_async(client.close())


def check_credentials(username: str, password: str) -> bool:
    # Simple Streamlit-secret credentials as requested.
    return username == settings.app_username and password == settings.app_password


def ensure_store():
    if st.session_state.get("store_initialized"):
        return
    store.initialize()
    st.session_state.store_initialized = True


def login_screen():
    inject_css()
    st.markdown(
        f'''<div class="login-wrap">
            <img class="login-logo" src="{LOGO_URL}" />
            <div class="login-title">TNS Retail Intelligence</div>
            <div class="login-sub">Sign in to access company analytics</div>
        </div>''',
        unsafe_allow_html=True,
    )
    with st.form("login_form"):
        username = st.text_input("Username", autocomplete="username")
        password = st.text_input("Password", type="password", autocomplete="current-password")
        submitted = st.form_submit_button("Sign in", use_container_width=True)
    if submitted:
        if check_credentials(username.strip(), password):
            st.session_state.authenticated = True
            st.session_state.username = username.strip()
            st.session_state.active_chat_id = None
            st.rerun()
        else:
            st.error("Invalid username or password.")


def get_sessions():
    return store.list_sessions(st.session_state.username)


def load_history(session_id: str):
    history = store.get_history(session_id, st.session_state.username)
    if history is None:
        st.error("Chat session not found.")
        return None
    return history


def render_thoughts(thoughts):
    if not thoughts:
        return
    with st.expander("Thought process", expanded=False):
        for thought in thoughts:
            content = thought.get("content") if isinstance(thought, dict) else str(thought)
            if content:
                st.markdown(content)


def download_signed_links(table: Dict[str, Any], conversation_id: str, message_id: str):
    attachment_id = table.get("attachment_id")
    if not attachment_id or not message_id:
        return
    try:
        links = call_genie(lambda client: client.get_full_query_download_links(conversation_id, message_id, attachment_id))
        for index, link in enumerate(links):
            try:
                response = httpx.get(link, timeout=120.0)
                response.raise_for_status()
                label = "Download query result" if len(links) == 1 else f"Download query result {index + 1}"
                st.download_button(
                    label,
                    data=response.content,
                    file_name=f"genie_query_result_{index + 1}.csv",
                    mime="text/csv",
                    key=f"download_{conversation_id}_{message_id}_{attachment_id}_{index}",
                )
            except Exception as exc:
                logger.warning("Could not download query result: %s", exc)
                st.link_button(f"Open download {index + 1}", link)
    except Exception as exc:
        logger.warning("Could not create query-result download: %s", exc)


def render_table(table: Dict[str, Any], conversation_id: str, message_id: str):
    with st.container(border=True):
        st.markdown(f"**{table.get('title') or 'Query result'}**")
        if table.get("description"):
            st.caption(table["description"])
        columns = table.get("columns") or []
        rows = table.get("rows") or []
        if columns:
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info(table.get("error") or "No tabular result was returned.")
        displayed = table.get("displayed_row_count", len(rows))
        total = table.get("row_count", displayed)
        if table.get("truncated") or total > displayed:
            st.caption(f"Showing {displayed:,} of {total:,} rows.")
        download_signed_links(table, conversation_id, message_id)


def render_visualization(viz: Dict[str, Any], conversation_id: str, message_id: str):
    with st.container(border=True):
        st.markdown(f"**{viz.get('title') or 'Visualization'}**")
        attachment_id = viz.get("attachment_id")
        if not attachment_id or not message_id:
            return
        try:
            image = call_genie(lambda client: client.download_visualization(conversation_id, message_id, attachment_id))
            st.image(image, use_container_width=True)
            st.download_button(
                "Download chart",
                data=image,
                file_name="genie_visualization.png",
                mime="image/png",
                key=f"viz_download_{conversation_id}_{message_id}_{attachment_id}",
            )
        except Exception as exc:
            logger.warning("Visualization retrieval failed: %s", exc)
            st.warning("The analytical response was returned, but this visualization could not be loaded.")


def render_presentation(presentation: Dict[str, Any], conversation_id: str, message_id: str):
    if not presentation:
        return

    thoughts = presentation.get("thoughts") or []
    if not thoughts:
        for block in presentation.get("blocks") or []:
            if block.get("type") == "thoughts":
                thoughts = block.get("data") or []
                break
    render_thoughts(thoughts)

    visualizations = presentation.get("visualizations") or []
    if not visualizations:
        visualizations = [b.get("data") for b in presentation.get("blocks") or [] if b.get("type") == "visualization"]
    for viz in visualizations:
        render_visualization(viz, conversation_id, message_id)

    tables = presentation.get("tables") or []
    if not tables:
        tables = [b.get("data") for b in presentation.get("blocks") or [] if b.get("type") == "table"]
    chart_source_ids = {
        v.get("query_attachment_id") for v in visualizations if v.get("query_attachment_id")
    }
    for table in tables:
        if table.get("attachment_id") in chart_source_ids:
            continue
        render_table(table, conversation_id, message_id)

    suggestions = presentation.get("suggested_questions") or []
    if not suggestions:
        for block in presentation.get("blocks") or []:
            if block.get("type") == "suggested_questions":
                suggestions = block.get("data") or []
                break
    if suggestions:
        st.markdown('<div class="section-label">Suggested questions</div>', unsafe_allow_html=True)
        for index, question in enumerate(suggestions):
            if st.button(question, key=f"suggestion_{message_id}_{index}", use_container_width=True):
                st.session_state.pending_prompt = question
                st.rerun()


def render_message(message: Dict[str, Any], conversation_id: str):
    role = message.get("role")
    content = message.get("content") or ""
    if role == "user":
        st.markdown(f'<div class="user-bubble">{content.replace(chr(10), "<br>")}</div>', unsafe_allow_html=True)
        return

    st.markdown("<div class='assistant-bubble'>", unsafe_allow_html=True)
    st.markdown(content)
    st.markdown("</div>", unsafe_allow_html=True)
    presentation = message.get("presentation") or message.get("metadata") or {}
    message_id = presentation.get("agent_message_id") or message.get("message_id") or ""
    render_presentation(presentation, conversation_id, message_id)


async def run_agent_turn(client: GenieClient, message: str, conversation_id: Optional[str]):
    original = conversation_id
    rebound = False
    try:
        response = await client.create_agent_response(message, conversation_id=conversation_id, enable_visualization=True)
    except GenieError as exc:
        if conversation_id and client.is_legacy_conversation_error(exc):
            rebound = True
            response = await client.create_agent_response(message, conversation_id=None, enable_visualization=True)
        else:
            raise

    resolved = response.get("conversation_id") or (None if rebound else conversation_id)
    if not resolved:
        raise GenieError("Genie Agent did not return a conversation ID.")

    answer = client.normalize_answer_text(client.extract_agent_answer(response))
    presentation = await client.build_agent_presentation(response)
    message_id = presentation.get("agent_message_id") or response.get("id") or ""
    return {
        "answer": answer,
        "presentation": presentation,
        "conversation_id": resolved,
        "conversation_changed": resolved != original,
        "message_id": message_id,
    }


def process_new_message(prompt: str):
    client = new_client()
    try:
        result = run_async(run_agent_turn(client, prompt, None))
    finally:
        run_async(client.close())
    title = prompt.strip()
    if len(title) > 60:
        title = title[:57] + "..."
    sid = store.create_session(result["conversation_id"], st.session_state.username, title)
    store.add_message(sid, st.session_state.username, "user", prompt)
    store.add_message(sid, st.session_state.username, "assistant", result["answer"], result["presentation"])
    st.session_state.active_chat_id = sid
    return result


def process_followup(session_id: str, prompt: str):
    conversation_id = store.get_conversation_id(session_id, st.session_state.username)
    if not conversation_id:
        raise GenieError("Chat session not found.")
    client = new_client()
    try:
        result = run_async(run_agent_turn(client, prompt, conversation_id))
    finally:
        run_async(client.close())
    if result["conversation_changed"]:
        store.set_conversation_id(session_id, st.session_state.username, result["conversation_id"])
    store.add_message(session_id, st.session_state.username, "user", prompt)
    store.add_message(session_id, st.session_state.username, "assistant", result["answer"], result["presentation"])
    return result


def render_sidebar():
    with st.sidebar:
        st.markdown(
            f'<div class="tns-brand"><img src="{LOGO_URL}"/><div class="tns-brand-name">TNS Retail Intelligence</div></div>',
            unsafe_allow_html=True,
        )
        if st.button("＋  New Chat", use_container_width=True):
            st.session_state.active_chat_id = None
            st.session_state.pending_prompt = ""
            st.rerun()
        st.markdown('<div class="section-label">Conversations</div>', unsafe_allow_html=True)
        sessions = get_sessions()
        for session in sessions:
            label = session["title"] or "New Chat"
            c1, c2 = st.columns([0.86, 0.14], gap="small")
            with c1:
                if st.button(label, key=f"chat_{session['session_id']}", use_container_width=True):
                    st.session_state.active_chat_id = session["session_id"]
                    st.session_state.pending_prompt = ""
                    st.rerun()
            with c2:
                if st.button("×", key=f"delete_{session['session_id']}", help="Delete conversation"):
                    store.delete_session(session["session_id"], st.session_state.username)
                    if st.session_state.get("active_chat_id") == session["session_id"]:
                        st.session_state.active_chat_id = None
                    st.rerun()
        st.divider()
        st.caption(st.session_state.username)
        if st.button("Logout", use_container_width=True):
            st.session_state.clear()
            st.rerun()


def main_app():
    inject_css()
    ensure_store()
    render_sidebar()

    active_id = st.session_state.get("active_chat_id")
    history = load_history(active_id) if active_id else None
    title = history["title"] if history else "New Chat"

    st.markdown(
        f'''<div class="tns-header"><div class="tns-header-title">{title}</div><div class="tns-header-subtitle">Business Intelligence Assistant</div></div>''',
        unsafe_allow_html=True,
    )

    if history:
        conversation_id = store.get_conversation_id(active_id, st.session_state.username)
        for message in history["messages"]:
            render_message(message, conversation_id)
    else:
        st.markdown(
            '''<div class="welcome"><h1>How can I help you?</h1><p>Ask questions about company sales, products, stores, brands and more.</p></div>''',
            unsafe_allow_html=True,
        )
    pending = st.session_state.pop("pending_prompt", "")
    prompt = st.chat_input("Ask a question...", max_chars=5000)
    if pending:
        prompt = pending

    if prompt:
        with st.spinner("Genie is analyzing your request..."):
            try:
                if active_id:
                    result = process_followup(active_id, prompt)
                else:
                    result = process_new_message(prompt)
                st.rerun()
            except Exception as exc:
                logger.exception("Genie request failed")
                st.error(f"Request failed: {exc}")


if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "pending_prompt" not in st.session_state:
    st.session_state.pending_prompt = ""

if st.session_state.authenticated:
    try:
        main_app()
    except Exception as exc:
        logger.exception("Application error")
        st.error(f"Application error: {exc}")
else:
    login_screen()
