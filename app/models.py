from typing import Any

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=1, max_length=500)


class LoginResponse(BaseModel):
    status: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=5000)


class ChatResponse(BaseModel):
    session_id: str
    message_id: str
    status: str
    answer: str
    presentation: dict[str, Any] = Field(default_factory=dict)


class ChatSessionSummary(BaseModel):
    session_id: str
    title: str
    created_at: str
    last_activity_at: str


class ChatMessage(BaseModel):
    role: str
    content: str
    created_at: str
    presentation: dict[str, Any] = Field(default_factory=dict)


class ChatHistoryResponse(BaseModel):
    session_id: str
    title: str
    created_at: str
    last_activity_at: str
    messages: list[ChatMessage]


