# maha-s3-mcp-server

A server application for managing S3-compatible storage and MCP (Multi-Cloud Platform) operations.

## Project Structure

```
├── src/                        # Source code for the server
│   └── s3_utils/
│       └── s3_file_transfer.py # Handles file upload/download operations with S3
│       └── s3_functions.py     # Utility functions for S3 bucket and object management
├── s3_mcp_server.py            # Main server application entry point
├── streamlit_chat_bot.py       # Streamlit app for interactive Q&A with file content
├── .env                        # Environment variable definitions (not committed)
├── docker-compose.yml          # Docker Compose configuration for multi-container setup
├── Dockerfile.s3               # Dockerfile for building the S3 server image
├── Dockerfile.streamlit        # Dockerfile for building the Streamlit chatbot image
├── README.md                   # Project documentation
├── requirements.txt            # Python dependencies
```

## Getting Started

1. **Configure environment:**
    - rename `example.env` to `.env` and update values as needed.

2. **Run Docker:**
    ```bash
    docker compose up --build -d
    ```

## Features
1. Bucket Management: List and explore S3 buckets
2. Object Operations: Retrieve objects and metadata from buckets
3. File Download: Securely download files for processing
4. Intelligent Q&A: Read and analyze file content to answer questions


## License

MIT  