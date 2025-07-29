import asyncio
import streamlit as st
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerSSE
import os
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, List, Any
import logging

# Load environment variables
load_dotenv()

# Environment variables
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# MCP Server configurations
MCP_SERVERS = {
    "s3": {
        "name": "S3 MCP Server",
        "host": os.getenv("S3_MCP_SERVER_HOST", "s3-mcp-server"),
        "port": os.getenv("S3_MCP_SERVER_PORT", "8001"),
        "url": None,  # Will be constructed
        "tool_prefix": "s3_functionalities",
        "description": "AWS S3 bucket operations and management"
    },
    "deepwiki": {
        "name": "DeepWiki MCP Server",
        "host": None,
        "port": None,
        "url": "https://mcp.deepwiki.com/mcp",
        "tool_prefix": "deepwiki_search",
        "description": "Wikipedia and knowledge base search"
    }
}

# Construct URLs for servers that need them
for server_key, server_config in MCP_SERVERS.items():
    if server_config["url"] is None and server_config["host"] and server_config["port"]:
        server_config["url"] = f"http://{server_config['host']}:{server_config['port']}/sse"

# Model configurations
OPENAI_MODELS = [
    "gpt-4o",
    "gpt-4-turbo", 
    "gpt-4",
    "gpt-3.5-turbo"
]

ANTHROPIC_MODELS = [
    "claude-sonnet-4-20250514",
    "claude-opus-4-20250514",
    "claude-3-7-sonnet-20250219"
]

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_mcp_server(server_config: Dict[str, Any]) -> MCPServerSSE:
    """Create an MCP server instance from configuration."""
    try:
        return MCPServerSSE(
            url=server_config["url"], 
            tool_prefix=server_config["tool_prefix"]
        )
    except Exception as e:
        logger.error(f"Failed to create MCP server {server_config['name']}: {e}")
        return None

@st.cache_resource
def setup_agent(llm_provider: str, model: str, selected_servers: List[str]):
    """Setup agent with selected MCP servers."""
    toolsets = []
    
    for server_key in selected_servers:
        if server_key in MCP_SERVERS:
            server_config = MCP_SERVERS[server_key]
            mcp_server = create_mcp_server(server_config)
            if mcp_server:
                toolsets.append(mcp_server)
                logger.info(f"Added MCP server: {server_config['name']}")
            else:
                st.warning(f"Failed to connect to {server_config['name']}")
    
    agent_id = f"{llm_provider}:{model}"
    
    if not toolsets:
        st.error("No MCP servers could be connected. Please check your configuration.")
        return None
    
    return Agent(agent_id, toolsets=toolsets)

async def run_agent_query(agent: Agent, query: str) -> str:
    """Run a query against the agent."""
    try:
        async with agent:
            result = await agent.run(query)
            return result.output
    except Exception as e:
        logger.error(f"Error running agent query: {e}")
        return f"Error: {str(e)}"

def main():
    st.title("🤖 Multi-MCP Server AI Agent")
    st.markdown("Connect to multiple MCP servers and interact with various tools and services.")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # LLM Provider selection
        llm_provider = st.selectbox(
            "Select LLM Provider",
            options=["anthropic", "openai"],
            index=0,
            help="Choose your preferred language model provider"
        )
        
        # Model selection based on provider
        models = ANTHROPIC_MODELS if llm_provider == "anthropic" else OPENAI_MODELS
        selected_model = st.selectbox(
            "Select Model",
            options=models,
            index=0,
            help="Choose the specific model to use"
        )
        
        st.divider()
        
        # MCP Server selection
        st.subheader("🔗 MCP Servers")
        selected_servers = []
        
        for server_key, server_config in MCP_SERVERS.items():
            is_selected = st.checkbox(
                server_config["name"],
                value=(server_key == "s3"),  # Default to S3 selected
                key=f"server_{server_key}",
                help=server_config["description"]
            )
            if is_selected:
                selected_servers.append(server_key)
        
        if not selected_servers:
            st.warning("⚠️ Please select at least one MCP server")
        
        st.divider()
        
        # Display current configuration
        st.subheader("📋 Current Configuration")
        st.write(f"**Agent:** `{llm_provider}:{selected_model}`")
        st.write(f"**Active Servers:** {len(selected_servers)}")
        for server_key in selected_servers:
            st.write(f"- {MCP_SERVERS[server_key]['name']}")
        
        # API Key status
        api_key = ANTHROPIC_API_KEY if llm_provider == "anthropic" else OPENAI_API_KEY
        if api_key:
            st.success(f"✅ {llm_provider.upper()} API Key loaded")
        else:
            st.error(f"❌ {llm_provider.upper()} API Key missing")
    
    # Main content area
    if not selected_servers:
        st.warning("Please select at least one MCP server from the sidebar to continue.")
        return
    
    # Initialize session state for chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Setup agent
    with st.spinner("🔄 Setting up agent and connecting to MCP servers..."):
        agent = setup_agent(llm_provider, selected_model, selected_servers)
    
    if not agent:
        st.error("Failed to setup agent. Please check your configuration and try again.")
        return
    
    st.success(f"✅ Agent ready with {len(selected_servers)} MCP server(s)")
    
    # Sample queries section
    with st.expander("💡 Sample Queries", expanded=False):
        st.markdown("""
        Here are some example queries you can try:
        
        **For S3 Server:**
        - "How many buckets are present in the S3 server?"
        - "List all files in bucket 'my-bucket'"
        - "Create a new bucket called 'test-bucket'"
        
        **For DeepWiki Server:**
        - "Search for information about artificial intelligence"
        - "What is quantum computing?"
       
            """)
    
    # Chat interface
    st.subheader("💬 Chat with AI Agent")
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask me anything about your connected services..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("🤔 Thinking..."):
                response = asyncio.run(run_agent_query(agent, prompt))
            st.markdown(response)
        
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response})
    
    # Clear chat button
    if st.button("🗑️ Clear Chat History"):
        st.session_state.messages = []
        st.rerun()
    
    # Footer
    st.divider()
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <small>Multi-MCP Server AI Agent | Built with Streamlit & PydanticAI</small>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()