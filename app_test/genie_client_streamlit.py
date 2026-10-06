import asyncio
import inspect
import json
import logging
import re
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

from streamlit_runtime import settings, databricks_auth

logger = logging.getLogger("app.genie")


class GenieError(Exception):
    pass


class GenieStreamInterrupted(GenieError):
    """The SSE stream/connection died before the agent run completed.

    This is the only error class that create_agent_response() retries,
    because it means no complete answer was received.
    """


# HTTP statuses on the streaming endpoint that are worth retrying.
_RETRYABLE_STATUS = {502, 503, 504}

# Transport-level failures (ReadError, RemoteProtocolError, ConnectError,
# WriteError, ...). httpx.ReadError is NOT only a "timeout" - it is raised
# when the peer drops the connection mid-body, which is exactly what killed
# the long-running forecast response.
_TRANSPORT_ERRORS = (httpx.TransportError,)


class GenieClient:
    """Databricks Genie Agent client.

    The web application uses the same Agent-mode API as the Databricks
    Genie Agent experience. The Agent response (reasoning, SQL/tool calls,
    query outputs, final report, citations) is returned without asking
    another model to rewrite it.

    Reliability notes
    -----------------
    * Each Agent run uses its OWN httpx client/connection, so a dropped
      long-lived SSE connection can never poison the shared connection pool
      used by visualization / query-result downloads.
    * TCP keep-alive is enabled on that connection so firewalls/NAT/Windows
      do not silently drop it during minutes of "silent" agent computation.
    * A stream that ends before `response.completed` is NEVER presented as
      a final answer. It is retried (settings.genie_stream_retries) and,
      if it keeps failing, surfaced as a clear error.
    """

    def __init__(self) -> None:
        self.host = settings.databricks_host.rstrip("/")
        self.agent_id = settings.genie_space_id
        self.client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _timeout_seconds() -> float:
        return max(
            1800.0,
            float(getattr(settings, "genie_timeout_seconds", 300)),
        )

    @staticmethod
    def _stream_retries() -> int:
        return max(0, int(getattr(settings, "genie_stream_retries", 2)))

    @staticmethod
    def _socket_options() -> List[tuple]:
        """TCP keep-alive options so idle SSE connections are not dropped."""
        if not getattr(settings, "genie_tcp_keepalive", True):
            return []

        options: List[tuple] = [
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ]
        for name, value in (
            ("TCP_KEEPIDLE", 30),
            ("TCP_KEEPINTVL", 10),
            ("TCP_KEEPCNT", 6),
        ):
            option = getattr(socket, name, None)
            if option is not None:
                options.append((socket.IPPROTO_TCP, option, value))
        return options

    def _new_client(
        self,
        *,
        max_connections: int = 20,
        max_keepalive_connections: int = 10,
    ) -> httpx.AsyncClient:
        timeout_seconds = self._timeout_seconds()
        transport = httpx.AsyncHTTPTransport(
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            socket_options=self._socket_options(),
        )
        return httpx.AsyncClient(
            transport=transport,
            timeout=httpx.Timeout(
                timeout_seconds,
                connect=30.0,
            ),
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Shared client for short requests (downloads, message lookups)."""
        if self.client is None or self.client.is_closed:
            self.client = self._new_client()
        return self.client

    async def _headers(
        self,
        *,
        accept: str = "application/json",
    ) -> Dict[str, str]:
        token = await databricks_auth.get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": accept,
        }

    @staticmethod
    async def _invalidate_token(authorization_header: str) -> None:
        invalidate = getattr(databricks_auth, "invalidate_token", None)
        if invalidate is None:
            return
        result = invalidate(authorization_header.removeprefix("Bearer "))
        if inspect.isawaitable(result):
            await result

    # ------------------------------------------------------------------
    # Short (non-streaming) requests
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        url: str,
        *,
        json_body: Optional[dict] = None,
        params: Optional[dict] = None,
        accept: str = "application/json",
        retry_401: bool = True,
    ) -> httpx.Response:
        client = await self._get_client()
        headers = await self._headers(accept=accept)

        try:
            response = await client.request(
                method,
                url,
                headers=headers,
                json=json_body,
                params=params,
            )
        except httpx.HTTPError as exc:
            raise GenieError(
                f"Databricks Genie request failed: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

        if response.status_code == 401 and retry_401:
            await self._invalidate_token(headers["Authorization"])
            headers = await self._headers(accept=accept)
            try:
                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    json=json_body,
                    params=params,
                )
            except httpx.HTTPError as exc:
                raise GenieError(
                    "Databricks Genie request failed after token refresh: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

        if response.status_code >= 400:
            detail = response.text[:2000]
            raise GenieError(
                "Databricks Genie request failed: HTTP "
                f"{response.status_code}: {detail}"
            )

        return response

    @staticmethod
    def is_legacy_conversation_error(exc: GenieError) -> bool:
        """Return True only for Databricks' Chat-mode/Agent-mode mismatch."""
        text = str(exc).lower()
        return (
            "this conversation cannot be used with the responses endpoint"
            in text
            and "only agent mode conversations are supported"
            in text
        )

    # ------------------------------------------------------------------
    # Agent-mode streaming
    # ------------------------------------------------------------------

    async def _open_stream(
        self,
        client: httpx.AsyncClient,
        url: str,
        body: Dict[str, Any],
    ) -> httpx.Response:
        """Open the SSE stream, refreshing the OAuth token once on 401."""
        timeout = httpx.Timeout(self._timeout_seconds(), connect=30.0)

        for auth_attempt in range(2):
            headers = await self._headers(accept="text/event-stream")
            request = client.build_request(
                "POST",
                url,
                headers=headers,
                json=body,
                timeout=timeout,
            )

            try:
                response = await client.send(request, stream=True)
            except _TRANSPORT_ERRORS as exc:
                raise GenieStreamInterrupted(
                    "Could not open Genie Agent stream: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc

            if response.status_code == 401 and auth_attempt == 0:
                await response.aclose()
                await self._invalidate_token(headers["Authorization"])
                continue

            if response.status_code >= 400:
                detail = (
                    await response.aread()
                ).decode("utf-8", errors="replace")[:2000]
                await response.aclose()
                message = (
                    "Databricks Genie Agent request failed: HTTP "
                    f"{response.status_code}: {detail}"
                )
                if response.status_code in _RETRYABLE_STATUS:
                    raise GenieStreamInterrupted(message)
                raise GenieError(message)

            return response

        raise GenieError("Databricks rejected the OAuth token twice.")

    async def _stream_agent_once(
        self,
        url: str,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Run ONE Agent response over a dedicated connection."""
        timeout_seconds = self._timeout_seconds()
        started = time.monotonic()

        events: List[Dict[str, Any]] = []
        final_response: Optional[Dict[str, Any]] = None
        event_name: Optional[str] = None
        data_lines: List[str] = []

        def consume_event() -> None:
            nonlocal event_name, data_lines, final_response

            if not data_lines:
                event_name = None
                return

            raw_data = "\n".join(data_lines)
            data_lines = []

            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError:
                event_name = None
                return

            if not isinstance(data, dict):
                event_name = None
                return

            actual_type = data.get("type") or event_name
            data["_event"] = actual_type
            events.append(data)

            if actual_type == "response.completed":
                final_response = data.get("response") or {}
            elif actual_type == "response.failed":
                failed = data.get("response") or {}
                error = failed.get("error") or data.get("error")
                raise GenieError(
                    f"Genie Agent response failed: {error or failed}"
                )

            event_name = None

        # Dedicated client: one long-lived SSE connection must not share a
        # pool with other requests.
        async with self._new_client(
            max_connections=2,
            max_keepalive_connections=0,
        ) as client:
            stream = await self._open_stream(client, url, body)

            try:
                async with asyncio.timeout(timeout_seconds):
                    async for line in stream.aiter_lines():
                        if line == "":
                            consume_event()
                            continue

                        if line.startswith(":"):
                            # SSE comment / heartbeat.
                            continue

                        if line.startswith("event:"):
                            event_name = line[6:].strip()
                        elif line.startswith("data:"):
                            data_lines.append(line[5:].lstrip())

                    consume_event()

            except TimeoutError as exc:
                raise GenieError(
                    f"Genie Agent response timed out after "
                    f"{int(timeout_seconds)}s."
                ) from exc
            except _TRANSPORT_ERRORS as exc:
                # e.g. httpx.ReadError: the connection was dropped while
                # the agent was still working.
                raise GenieStreamInterrupted(
                    "Genie Agent stream was interrupted after "
                    f"{time.monotonic() - started:.0f}s and "
                    f"{len(events)} event(s): "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            finally:
                await stream.aclose()

        if final_response is None:
            # Accept a last response object only if it says it completed.
            # Never present a half-finished run as the final answer.
            for event in reversed(events):
                candidate = event.get("response")
                if isinstance(candidate, dict):
                    if candidate.get("status") == "completed":
                        final_response = candidate
                    break

        if not final_response:
            raise GenieStreamInterrupted(
                "Genie Agent stream ended before response.completed after "
                f"{time.monotonic() - started:.0f}s "
                f"({len(events)} event(s) received)."
            )

        if final_response.get("status") == "failed":
            raise GenieError(
                f"Genie Agent response failed: "
                f"{final_response.get('error') or 'unknown error'}"
            )

        logger.info(
            "Genie Agent run completed in %.1fs with %d event(s)",
            time.monotonic() - started,
            len(events),
        )

        final_response["events"] = events
        return final_response

    async def create_agent_response(
        self,
        message: str,
        conversation_id: Optional[str] = None,
        enable_visualization: bool = True,
    ) -> Dict[str, Any]:
        """Run one Genie Agent-mode response and collect the SSE stream.

        Interrupted streams are retried on a fresh connection. Only a
        stream that reached `response.completed` is returned.
        """
        url = f"{self.host}/api/2.0/genie/agents/{self.agent_id}/responses"

        body: Dict[str, Any] = {
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": message,
                        }
                    ],
                }
            ],
            "enable_viz": enable_visualization,
        }

        if conversation_id:
            body["conversation_id"] = conversation_id

        attempts = 1 + self._stream_retries()
        last_error: Optional[GenieStreamInterrupted] = None

        for attempt in range(1, attempts + 1):
            try:
                return await self._stream_agent_once(url, body)
            except GenieStreamInterrupted as exc:
                last_error = exc
                logger.warning(
                    "Genie stream attempt %d/%d failed: %s",
                    attempt,
                    attempts,
                    exc,
                )
                if attempt < attempts:
                    await asyncio.sleep(min(2.0 * attempt, 6.0))

        raise GenieError(
            f"Databricks Genie connection was interrupted on all "
            f"{attempts} attempt(s). Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Response extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _assistant_messages(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            item
            for item in (response.get("output") or [])
            if isinstance(item, dict)
            and item.get("type") == "message"
            and item.get("role") == "assistant"
        ]

    @staticmethod
    def normalize_answer_text(text: str) -> str:
        """Apply display-level fixes to the agent report text.

        The agent sometimes writes "$109,987" for Indian Rupee amounts.
        Replace a "$" that directly precedes a number with the configured
        currency symbol (default "₹"). Nothing else is rewritten.
        """
        symbol = getattr(settings, "genie_currency_symbol", "₹")
        if not symbol or not text:
            return text
        return re.sub(r"\$(?=\s?\d)", symbol, text)

    @staticmethod
    def _dump_debug(kind: str, conversation_id: Optional[str], payload: Any) -> None:
        directory = getattr(settings, "genie_debug_dump_dir", "")
        if not directory:
            return
        try:
            folder = Path(directory)
            folder.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            name = f"{stamp}_{kind}_{conversation_id or 'no-conversation'}.json"
            (folder / name).write_text(
                json.dumps(payload, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except Exception as exc:  # debugging must never break a request
            logger.warning("Could not write Genie debug dump: %s", exc)

    @staticmethod
    def extract_agent_answer(response: Dict[str, Any]) -> str:
        """Return the final assistant report exactly as Agent mode produced it."""
        messages = GenieClient._assistant_messages(response)

        # The final assistant message is the authoritative report.
        if messages:
            message = messages[-1]
            chunks = []
            for content in message.get("content") or []:
                if not isinstance(content, dict):
                    continue
                if content.get("type") == "output_text":
                    text = content.get("text")
                    if text:
                        chunks.append(text)

            answer = "".join(chunks).strip()
            if answer:
                return answer

        # Fallback to any output_text item without rewriting it.
        chunks = []
        for item in response.get("output") or []:
            if not isinstance(item, dict) or item.get("type") != "message":
                continue
            for content in item.get("content") or []:
                if (
                    isinstance(content, dict)
                    and content.get("type") == "output_text"
                    and content.get("text")
                ):
                    chunks.append(content["text"])

        answer = "".join(chunks).strip()
        if answer:
            return answer

        raise GenieError(
            "Genie Agent completed successfully, but no assistant report was returned."
        )

    @staticmethod
    def extract_agent_reasoning(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        reasoning: List[Dict[str, Any]] = []
        for item in response.get("output") or []:
            if not isinstance(item, dict) or item.get("type") != "reasoning":
                continue
            for content in item.get("content") or []:
                if not isinstance(content, dict):
                    continue
                if content.get("type") == "reasoning_text" and content.get("text"):
                    reasoning.append({
                        "type": "reasoning",
                        "content": content["text"],
                    })
        return reasoning

    @staticmethod
    def extract_agent_sql(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        sql_items: List[Dict[str, Any]] = []
        for item in response.get("output") or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "function_call":
                continue
            if item.get("name") != "execute_sql":
                continue

            arguments = item.get("arguments")
            parsed: Dict[str, Any]
            if isinstance(arguments, str):
                try:
                    parsed = json.loads(arguments)
                except json.JSONDecodeError:
                    parsed = {"raw_arguments": arguments}
            elif isinstance(arguments, dict):
                parsed = arguments
            else:
                parsed = {}

            sql_items.append({
                "call_id": item.get("call_id"),
                "title": parsed.get("title") or "SQL query",
                "sql": parsed.get("sql") or "",
                "status": item.get("status"),
            })
        return sql_items

    @staticmethod
    def extract_agent_query_results(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Keep the native function_call_output markdown result when available."""
        results: List[Dict[str, Any]] = []
        for item in response.get("output") or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "function_call_output":
                continue

            output = item.get("output")
            if not output:
                continue

            results.append({
                "call_id": item.get("call_id"),
                "content": output,
                "status": item.get("status"),
            })
        return results

    @staticmethod
    def extract_agent_citations(response: Dict[str, Any]) -> List[str]:
        """Extract citation URLs already present in Genie output without altering them."""
        citations: List[str] = []
        for item in response.get("output") or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "message":
                continue
            for content in item.get("content") or []:
                if not isinstance(content, dict):
                    continue
                for annotation in content.get("annotations") or []:
                    if not isinstance(annotation, dict):
                        continue
                    url = annotation.get("url")
                    if url and url not in citations:
                        citations.append(url)
        return citations

    # ------------------------------------------------------------------
    # Conversation API projection (attachments, visualizations)
    # ------------------------------------------------------------------

    async def get_agent_messages(
        self,
        conversation_id: str,
        *,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """Return the conversation message projections for an Agent conversation.

        Agent-mode responses contain the reasoning/tool transcript, while the
        Conversation API message projection contains the Genie message
        attachments (including visualization attachments). We need both.
        """
        url = (
            f"{self.host}/api/2.0/genie/spaces/"
            f"{self.agent_id}/conversations/{conversation_id}/messages"
        )

        response = await self._request(
            "GET",
            url,
            params={"page_size": page_size},
        )
        payload = response.json()
        return list(payload.get("messages") or [])

    @staticmethod
    def _message_time(message: Dict[str, Any]) -> Optional[float]:
        """Best-effort creation time of a Genie message, in seconds."""
        for key in (
            "created_timestamp",
            "created_at",
            "last_updated_timestamp",
            "updated_at",
        ):
            value = message.get(key)
            if value is None or value == "":
                continue
            if isinstance(value, (int, float)):
                number = float(value)
                # Genie uses epoch milliseconds.
                return number / 1000.0 if number > 1e11 else number
            if isinstance(value, str):
                if value.isdigit():
                    number = float(value)
                    return number / 1000.0 if number > 1e11 else number
                try:
                    return datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    ).timestamp()
                except ValueError:
                    continue
        return None

    @classmethod
    def _pick_latest_message(
        cls,
        messages: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Pick the NEWEST message, independent of the API's list order.

        The previous implementation assumed the list was oldest-first and
        took the last COMPLETED item. If Databricks returns newest-first,
        that picks the OLDEST message, and the answer ends up with charts
        and tables from earlier questions in the same conversation.
        """
        candidates = [m for m in messages if isinstance(m, dict)]
        if not candidates:
            return None

        def is_completed(message: Dict[str, Any]) -> bool:
            return str(message.get("status", "")).upper() == "COMPLETED"

        completed = [m for m in candidates if is_completed(m)]
        pool = completed or candidates

        timed = [
            (cls._message_time(m), index, m)
            for index, m in enumerate(pool)
        ]

        if all(item[0] is not None for item in timed):
            return max(timed, key=lambda item: (item[0], item[1]))[2]

        # No usable timestamps: keep the historical assumption that the
        # list is oldest-first.
        return pool[-1]

    async def get_latest_agent_message(
        self,
        conversation_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Find the newest assistant message of an Agent conversation.

        The attachment projection can lag slightly behind the SSE stream,
        so wait briefly while the newest message is not yet COMPLETED.
        """
        message: Optional[Dict[str, Any]] = None

        for attempt in range(4):
            messages = await self.get_agent_messages(conversation_id)
            self._dump_debug("messages", conversation_id, messages)

            message = self._pick_latest_message(messages)
            if message is None:
                return None

            if str(message.get("status", "")).upper() == "COMPLETED":
                break

            await asyncio.sleep(1.0)

        if message is not None:
            logger.info(
                "Using Genie message %s (status=%s, attachments=%d)",
                message.get("message_id") or message.get("id"),
                message.get("status"),
                len(message.get("attachments") or []),
            )

        return message

    async def build_agent_presentation(
        self,
        response: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the frontend envelope while preserving native Agent output.

        Agent-mode SSE contains reasoning, SQL calls, query outputs and the
        final report. The separate Conversation API message projection
        contains the visualization attachment generated for that report.
        Fetch it here so the existing frontend image endpoint can render the
        same Genie chart that appears inside Databricks.

        A failure while fetching attachments must never discard the answer
        the agent already produced, so those errors are logged and skipped.
        """
        reasoning = self.extract_agent_reasoning(response)
        sql_items = self.extract_agent_sql(response)
        query_results = self.extract_agent_query_results(response)
        citations = self.extract_agent_citations(response)

        blocks: List[Dict[str, Any]] = []
        visualizations: List[Dict[str, Any]] = []
        tables: List[Dict[str, Any]] = []
        suggested_questions: List[str] = []
        agent_message_id = ""
        conversation_id = response.get("conversation_id")
        self._dump_debug("response", conversation_id, response)

        if reasoning:
            blocks.append({
                "type": "thoughts",
                "data": reasoning,
            })

        # Agent-mode Conversation API message projection. This is deliberately
        # separate from the SSE response because visualization attachments are
        # exposed there and can be downloaded with the standard Genie endpoint.
        if conversation_id:
            try:
                agent_message = await self.get_latest_agent_message(
                    conversation_id
                )
            except GenieError as exc:
                logger.warning(
                    "Could not load Genie message attachments: %s", exc
                )
                agent_message = None

            if agent_message:
                agent_message_id = (
                    agent_message.get("message_id")
                    or agent_message.get("id")
                    or ""
                )

                for question in (
                    (agent_message.get("suggested_questions") or {}).get("questions")
                    or []
                ):
                    if question not in suggested_questions:
                        suggested_questions.append(question)

                for attachment in agent_message.get("attachments") or []:
                    if not isinstance(attachment, dict):
                        continue

                    attachment_id = attachment.get("attachment_id")
                    query = attachment.get("query") or {}
                    viz = attachment.get("viz") or {}

                    if query and attachment_id and agent_message_id:
                        # Keep the query attachment available for the existing
                        # table/download UI. The actual result is fetched by
                        # the existing query-result endpoint.
                        table = await self._build_table(
                            query,
                            attachment_id,
                            conversation_id,
                            agent_message_id,
                        )
                        if table:
                            tables.append(table)
                            blocks.append({
                                "type": "table",
                                "data": table,
                            })

                        for question in (
                            (attachment.get("suggested_questions") or {}).get("questions")
                            or []
                        ):
                            if question not in suggested_questions:
                                suggested_questions.append(question)

                    if viz and agent_message_id:
                        viz_attachment_id = (
                            viz.get("attachment_id")
                            or attachment_id
                        )
                        if viz_attachment_id:
                            visualization = {
                                "title": viz.get("title") or "Visualization",
                                "attachment_id": viz_attachment_id,
                                "query_attachment_id": viz.get(
                                    "query_attachment_id"
                                ),
                                "image_url": (
                                    f"/api/genie/visualizations/"
                                    f"{conversation_id}/{agent_message_id}/"
                                    f"{viz_attachment_id}"
                                ),
                            }
                            visualizations.append(visualization)
                            blocks.append({
                                "type": "visualization",
                                "data": visualization,
                            })

        if sql_items:
            blocks.append({
                "type": "agent_sql",
                "data": sql_items,
            })

        if query_results:
            blocks.append({
                "type": "agent_query_results",
                "data": query_results,
            })

        if suggested_questions:
            blocks.append({
                "type": "suggested_questions",
                "data": suggested_questions,
            })

        return {
            "mode": "agent",
            "blocks": blocks,
            "thoughts": reasoning,
            "sql": sql_items,
            "query_results": query_results,
            "citations": citations,
            "visualizations": visualizations,
            "tables": tables,
            "suggested_questions": suggested_questions,
            "agent_message_id": agent_message_id,
        }

    async def get_attachment_query_result(
        self,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ) -> Dict[str, Any]:
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.agent_id}/"
            f"conversations/{conversation_id}/messages/{message_id}/"
            f"attachments/{attachment_id}/query-result"
        )
        response = await self._request("GET", url)
        return response.json()

    async def download_visualization(
        self,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ) -> bytes:
        """Download the rendered PNG for an Agent-mode visualization."""
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.agent_id}/"
            f"conversations/{conversation_id}/messages/{message_id}/"
            f"attachments/{attachment_id}/download-visualization"
        )
        response = await self._request(
            "GET",
            url,
            accept="image/png",
        )
        return response.content

    async def generate_full_query_download(
        self,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ) -> Dict[str, Any]:
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.agent_id}/"
            f"conversations/{conversation_id}/messages/{message_id}/"
            f"attachments/{attachment_id}/downloads"
        )
        response = await self._request("POST", url)
        return response.json()

    async def get_full_query_download(
        self,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
        download_id: str,
        download_id_signature: str,
    ) -> Dict[str, Any]:
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.agent_id}/"
            f"conversations/{conversation_id}/messages/{message_id}/"
            f"attachments/{attachment_id}/downloads/{download_id}"
        )
        response = await self._request(
            "GET",
            url,
            params={"download_id_signature": download_id_signature},
        )
        return response.json()

    async def get_full_query_download_links(
        self,
        conversation_id: str,
        message_id: str,
        attachment_id: str,
    ) -> List[str]:
        created = await self.generate_full_query_download(
            conversation_id,
            message_id,
            attachment_id,
        )

        download_id = created.get("download_id")
        signature = created.get("download_id_signature")
        if not download_id or not signature:
            raise GenieError(
                "Databricks did not return a full-query download ID."
            )

        deadline = time.monotonic() + self._timeout_seconds()

        while time.monotonic() < deadline:
            payload = await self.get_full_query_download(
                conversation_id,
                message_id,
                attachment_id,
                download_id,
                signature,
            )
            statement = payload.get("statement_response") or payload
            state = ((statement.get("status") or {}).get("state"))

            result = statement.get("result") or {}
            links = [
                item.get("external_link")
                for item in (result.get("external_links") or [])
                if item.get("external_link")
            ]
            if links:
                return links

            if state in {"FAILED", "CANCELED", "CLOSED"}:
                error = statement.get("error") or payload.get("error")
                raise GenieError(
                    f"Full query-result download failed: {error or state}"
                )

            await asyncio.sleep(
                getattr(settings, "genie_poll_interval_seconds", 2)
            )

        raise GenieError("Full query-result download timed out.")

    async def _build_table(
        self,
        query: Dict[str, Any],
        attachment_id: str,
        conversation_id: str,
        message_id: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            raw = await self.get_attachment_query_result(
                conversation_id,
                message_id,
                attachment_id,
            )
        except GenieError as exc:
            return {
                "title": query.get("title") or "Query result",
                "description": query.get("description") or "",
                "attachment_id": attachment_id,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "truncated": False,
                "query": query.get("query") or "",
                "error": str(exc),
            }

        statement = raw.get("statement_response") or raw
        manifest = statement.get("manifest") or {}
        schema = manifest.get("schema") or {}
        columns_meta = schema.get("columns") or []
        columns = [
            column.get("name") or f"Column {index + 1}"
            for index, column in enumerate(columns_meta)
        ]

        result = statement.get("result") or {}
        data_array = result.get("data_array") or []

        display_limit = 1000
        rows = []
        for raw_row in data_array[:display_limit]:
            if isinstance(raw_row, list):
                rows.append([
                    raw_row[index] if index < len(raw_row) else None
                    for index in range(len(columns))
                ])
            elif isinstance(raw_row, dict):
                rows.append([
                    raw_row.get(column)
                    for column in columns
                ])
            else:
                rows.append(
                    [raw_row]
                    + [None] * max(0, len(columns) - 1)
                )

        return {
            "title": query.get("title") or "Query result",
            "description": query.get("description") or "",
            "attachment_id": attachment_id,
            "columns": columns,
            "rows": rows,
            "row_count": (
                manifest.get("total_row_count")
                or result.get("row_count")
                or len(rows)
            ),
            "truncated": bool(
                manifest.get("truncated")
                or result.get("truncated")
            ),
            "displayed_row_count": len(rows),
            "query": query.get("query") or "",
            "download_url": (
                f"/api/genie/query-results/"
                f"{conversation_id}/{message_id}/{attachment_id}/download"
            ),
        }

    async def close(self) -> None:
        if self.client is not None and not self.client.is_closed:
            await self.client.aclose()
        self.client = None


genie_client = GenieClient()