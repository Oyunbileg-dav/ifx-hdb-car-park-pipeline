#!/bin/bash

# IFX HDB Carpark Pipeline Setup Script
# This script helps you set up the pipeline on any device

set -e  # Exit on any error

echo "🚀 Setting up IFX HDB Carpark Pipeline..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    echo "Please install Python 3.8+ from https://python.org"
    exit 1
fi

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is required but not installed."
    echo "Please install Docker from https://docker.com"
    exit 1
fi

# Check if Docker Compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is required but not installed."
    echo "Please install Docker Compose"
    exit 1
fi

echo "✅ Prerequisites check passed"

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "📥 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "⚙️  Creating environment configuration..."
    cat > .env << EOF
# Database Configuration
DATABASE_URL=postgresql://ifx:ifx@localhost:5432/ifx

# Singapore Government API Endpoints
HDB_CARPARK_API_URL=https://api.data.gov.sg/v1/transport/carpark-availability
HDB_CARPARK_INFO_URL=https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c
EOF
    echo "✅ Created .env file with default configuration"
else
    echo "✅ .env file already exists"
fi

# Start database
echo "🐘 Starting PostgreSQL database..."
make up

# Wait for database to be ready
echo "⏳ Waiting for database to be ready..."
sleep 5

# Initialize database
echo "🗄️  Initializing database schema..."
make init

echo ""
echo "🎉 Setup complete! You can now run the pipeline:"
echo ""
echo "   make run          # Run the complete pipeline"
echo "   make logs         # View database logs"
echo "   make clean-db     # Clear all data"
echo "   make down         # Stop the database"
echo ""
echo "📊 Access database via Adminer: http://localhost:8080"
echo "   System: PostgreSQL, Server: db, User: ifx, Password: ifx, Database: ifx"
echo ""
echo "🔍 To activate the virtual environment in future sessions:"
echo "   source venv/bin/activate"
echo ""
