# TNS Retail Intelligence — Streamlit

This is the Streamlit migration of the working TNS Retail Intelligence Databricks Genie Agent application.

The migration keeps the same analytical engine and response model. FastAPI, browser JavaScript authentication, and HTTP application routes are replaced by Streamlit session state and widgets.

## Preserved functionality

- Databricks Genie **Agent Mode**
- Agent SSE response handling
- Long-running response reliability/retries
- Legacy Chat-mode → Agent-mode recovery
- Final Genie answer
- Thought/reasoning section
- Query-result tables
- Native Genie visualization retrieval
- Chart display and chart download
- Query-result download
- Suggested questions
- Follow-up questions using the same Genie conversation
- PostgreSQL-backed conversation history
- New Chat
- Conversation switching
- Conversation deletion through the same persistent store
- Simple username/password login
- Databricks OAuth credentials remain server-side in Streamlit Secrets

## Files

```text
streamlit_app.py
streamlit_runtime.py
genie_client_streamlit.py
streamlit_store.py
requirements-streamlit.txt
.streamlit/secrets.example.toml
```

## Streamlit Secrets

Create `.streamlit/secrets.toml` locally or configure the same values in Streamlit Cloud's Secrets manager.

See:

```text
.streamlit/secrets.example.toml
```

Required values:

```toml
DATABRICKS_HOST = "https://<workspace>.cloud.databricks.com"
DATABRICKS_CLIENT_ID = "<oauth-client-id>"
DATABRICKS_CLIENT_SECRET = "<oauth-client-secret>"
GENIE_SPACE_ID = "<genie-space-id>"

APP_USERNAME = "admin"
APP_PASSWORD = "change-me"

POSTGRES_HOST = "<postgres-host>"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "<database>"
POSTGRES_USER = "<user>"
POSTGRES_PASSWORD = "<password>"
POSTGRES_SCHEMA = "genie_app"
```

The PostgreSQL settings are retained because the original application persisted conversations and response presentation metadata in PostgreSQL. This prevents Streamlit migration from turning chat history into ephemeral browser state.

## Install

```bash
pip install -r requirements-streamlit.txt
```

## Run locally

```bash
streamlit run streamlit_app.py
```

## Streamlit Cloud

1. Push these files to GitHub.
2. Create a Streamlit Cloud app pointing to `streamlit_app.py`.
3. Open the app's **Settings → Secrets**.
4. Paste the values from your production secret configuration.
5. Deploy.

Do **not** commit `.streamlit/secrets.toml`.

## Conversation flow

```text
Streamlit login
      |
      v
PostgreSQL-backed chat history
      |
      v
Databricks OAuth
      |
      v
Genie Agent /responses
      |
      +--> final answer
      +--> reasoning
      +--> query results
      +--> visualizations
      +--> suggested questions
      |
      v
Streamlit presentation layer
```

## Important implementation detail

The Genie client is intentionally kept very close to the working production client. The Streamlit layer does not generate or rewrite the analytical response. It only replaces the FastAPI/HTML transport and renders the same Genie presentation objects.
