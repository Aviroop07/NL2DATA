# NL2DATA: Natural Language to Database Schema Generation System

A comprehensive system that automatically generates complete database schemas, DDL statements, and data generation strategies from natural language descriptions. The system uses Large Language Models (LLMs) orchestrated through LangGraph to transform high-level requirements into production-ready database designs.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Pipeline Overview](#pipeline-overview)
- [Detailed Phase Breakdown](#detailed-phase-breakdown)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Key Features](#key-features)
- [Implementation Details](#implementation-details)
- [Testing](#testing)

## Overview

NL2DATA is an end-to-end system that takes natural language descriptions of database requirements and automatically generates:

- **Entity-Relationship (ER) Diagrams**: Complete ER models with entities, relationships, and cardinalities
- **Relational Schemas**: Normalized relational database schemas (3NF)
- **DDL Statements**: Production-ready SQL DDL for schema creation
- **Data Generation Strategies**: Detailed strategies for generating synthetic data that matches the requirements
- **Constraints & Distributions**: Business rules, constraints, and data distribution specifications

The system is designed to handle complex, real-world database requirements including:
- Multi-entity relationships with various cardinalities
- Derived attributes and computed columns
- Functional dependencies and normalization
- Temporal attributes and time-series data
- Composite attributes and multi-valued attributes
- Complex constraints (check, unique, foreign key, nullability)
- Data distribution patterns (Zipf, log-normal, seasonal patterns)

## Architecture

The system follows a three-tier architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend (React + TypeScript)              │
│  - Natural Language Input Interface                          │
│  - Real-time Progress Tracking (WebSocket)                   │
│  - ER Diagram Visualization                                 │
│  - Relational Schema Editor                                  │
│  - Quality Metrics & Suggestions                             │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTP/WebSocket
┌────────────────────────▼────────────────────────────────────┐
│              Backend (FastAPI + Python)                      │
│  - REST API Endpoints                                        │
│  - WebSocket Server for Real-time Updates                   │
│  - Job Management & Status Tracking                         │
│  - Service Layer (NL2Data, Validation, Conversion)           │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│         NL2DATA Pipeline (LangGraph Orchestration)            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Phase 1: Domain & Entity Discovery                   │  │
│  │ Phase 2: Attribute Discovery & Schema Design         │  │
│  │ Phase 3: ER Design Compilation                       │  │
│  │ Phase 4: Relational Schema Compilation                │  │
│  │ Phase 5: Data Type Assignment                         │  │
│  │ Phase 6: DDL Generation & Schema Creation              │  │
│  │ Phase 7: Information Mining                           │  │
│  │ Phase 8: Functional Dependencies & Constraints        │  │
│  │ Phase 9: Constraints & Generation Strategies          │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Component Details

**Frontend (`front end/`)**
- Built with React 18, TypeScript, and Material-UI (MUI)
- Real-time WebSocket communication for live progress updates
- State management using Zustand
- Form validation with React Hook Form and Zod
- ER diagram and relational schema visualization/editing
- Checkpoint management UI for step-by-step execution
- Quality metrics and suggestions panel
- Auto-save functionality
- Responsive design with Material-UI components
- Type-safe API clients with TypeScript

**Backend (`backend/`)**
- FastAPI-based REST API with automatic OpenAPI documentation
- WebSocket support for bidirectional real-time communication
- Job management system for tracking long-running pipeline executions
- Checkpoint-based execution with state persistence
- Service layer abstraction for pipeline integration
- Request/response models with Pydantic validation
- ER diagram generation and serving
- Schema validation and conversion services
- AI-powered suggestion service
- Static file serving for generated diagrams
- Comprehensive request/response logging

**NL2DATA Pipeline (`NL2DATA/`)**
- LangGraph-based workflow orchestration
- Modular phase-based architecture
- State management through TypedDict (IRGenerationState)
- LLM integration with OpenAI (configurable models)
- Comprehensive validation and error handling

## Pipeline Overview

The NL2DATA pipeline consists of 9 sequential phases, each building upon the previous phase's output:

```
Phase 1: Domain & Entity Discovery
    ↓
Phase 2: Attribute Discovery & Schema Design
    ↓
Phase 3: ER Design Compilation
    ↓
Phase 4: Relational Schema Compilation
    ↓
Phase 5: Data Type Assignment
    ↓
Phase 6: DDL Generation & Schema Creation
    ↓
Phase 7: Information Mining
    ↓
Phase 8: Functional Dependencies & Constraints
    ↓
Phase 9: Constraints & Generation Strategies
```

Each phase is implemented as a LangGraph StateGraph, allowing for:
- Conditional branching based on intermediate results
- Parallel execution of independent steps
- Iterative refinement loops
- State persistence and checkpointing
- Error recovery and retry mechanisms

## Detailed Phase Breakdown

### Phase 1: Domain & Entity Discovery

**Purpose**: Extract the domain context and identify all entities and relationships from the natural language description.

**Steps**:
1. **Domain Detection (1.1)**: Identify the business domain (e.g., e-commerce, healthcare, IoT)
2. **Entity Mention Detection (1.2)**: Find all entity mentions in the text
3. **Domain Inference (1.3)**: Infer domain if not explicitly mentioned (conditional)
4. **Key Entity Extraction (1.4)**: Extract core entities with their properties
5. **Relation Mention Detection (1.5)**: Identify relationship mentions (parallel with 1.6)
6. **Auxiliary Entity Suggestion (1.6)**: Suggest supporting entities (parallel with 1.5)
7. **Entity Consolidation (1.7)**: Merge duplicate entities and resolve conflicts
8. **Entity vs Relation Reclassification (1.75)**: Correct misclassifications
9. **Entity Cardinality (1.8)**: Determine entity participation (parallel per entity)
10. **Key Relations Extraction (1.9)**: Extract relationships between entities
11. **Schema Connectivity (1.10)**: Ensure all entities are connected (loop if orphans found)
12. **Relation Cardinality (1.11)**: Determine relationship cardinalities (parallel per relation)
13. **Relation Validation (1.12)**: Validate relationships (loop if validation fails)

**Output**: 
- Domain classification
- List of entities with basic properties
- List of relationships with cardinalities
- Entity-relationship graph structure

**Key Logic**:
- Uses semantic similarity for entity consolidation
- Parallel processing for independent entity/relation operations
- Iterative loops for connectivity and validation
- Model routing: Critical steps use advanced reasoning models

### Phase 2: Attribute Discovery & Schema Design

**Purpose**: Discover all attributes for each entity, identify keys, constraints, and derived attributes.

**Steps**:
1. **Attribute Count Detection (2.1)**: Estimate number of attributes per entity
2. **Intrinsic Attributes (2.2)**: Extract core attributes for each entity
3. **Attribute Synonym Detection (2.3)**: Identify and merge duplicate attributes
4. **Cross-Entity Attribute Reconciliation (2.16)**: Resolve attribute conflicts across entities
5. **Composite Attribute Handling (2.4)**: Decompose composite attributes with DSL
6. **Temporal Attributes Detection (2.5)**: Identify time-based attributes
7. **Naming Convention Validation (2.6)**: Validate and standardize attribute names (loop if fails)
8. **Primary Key Identification (2.7)**: Identify primary keys for each entity
9. **Multivalued/Derived Detection (2.8)**: Classify attributes as multivalued or derived
10. **Derived Attribute Formulas (2.9)**: Extract formulas for derived attributes (DSL-based)
11. **Unique Constraints (2.10)**: Identify unique attribute combinations
12. **Nullability Constraints (2.11)**: Determine which attributes can be NULL
13. **Default Values (2.12)**: Assign default values where appropriate
14. **Check Constraints (2.13)**: Identify value range constraints
15. **Entity Cleanup (2.14)**: Final entity validation and cleanup (loop if incomplete)
16. **Relation Realization (2.14)**: Convert relationships to foreign keys
17. **Relation Intrinsic Attributes (2.15)**: Add attributes to relationship tables

**Output**:
- Complete attribute lists per entity
- Primary key definitions
- Foreign key relationships
- Constraint specifications
- Derived attribute formulas (DSL)
- Composite attribute decompositions

**Key Logic**:
- DSL (Domain-Specific Language) for derived attributes and composite decompositions
- Cross-entity reconciliation to ensure consistency
- Iterative naming validation
- Parallel attribute processing where possible

### Phase 3: ER Design Compilation

**Purpose**: Compile entities, relations, and attributes into a complete ER design representation.

**Steps**:
1. **ER Design Compilation (3.1)**: Compile complete ER design from Phase 1 & 2 results, including entities, relations, attributes, primary keys, foreign keys, and constraints
2. **Junction Table Naming (3.2)**: Generate appropriate names for many-to-many junction tables

**Output**:
- Complete ER design structure
- Entity-attribute mappings
- Junction table name mappings
- ER design metadata

**Key Logic**:
- Consolidates all Phase 1 and Phase 2 outputs into a unified ER design
- Handles many-to-many relationship table naming
- Prepares data structure for relational schema conversion

### Phase 4: Relational Schema Compilation

**Purpose**: Convert the ER design into a canonical relational schema representation.

**Steps**:
1. **Relational Schema Compilation (4.1)**: Convert ER design to normalized relational schema with tables, columns, keys, and constraints

**Output**:
- Complete relational schema structure
- Table definitions with columns
- Primary key and foreign key specifications
- Constraint definitions

**Key Logic**:
- ER-to-relational mapping
- Handles junction tables for many-to-many relationships
- Preserves all constraints and relationships
- Creates canonical schema representation

### Phase 5: Data Type Assignment

**Purpose**: Build attribute dependency graph and assign appropriate data types to all attributes.

**Steps**:
1. **Attribute Dependency Graph Construction (5.1)**: Build dependency graph for attributes (independent, foreign keys, derived)
2. **Independent Attribute Type Assignment (5.2)**: Assign data types to independent attributes based on domain and usage
3. **Foreign Key Type Derivation (5.3)**: Derive foreign key types from referenced primary key types
4. **Dependent Attribute Type Assignment (5.4)**: Assign types to derived and dependent attributes based on dependency graph

**Output**:
- Attribute dependency graph
- Complete data type assignments for all attributes
- Type information per table and column
- Nullability constraints

**Key Logic**:
- Dependency-aware type assignment
- Ensures foreign keys match referenced primary key types
- Handles derived attributes appropriately
- Schema is frozen after this phase (no structural changes)

### Phase 6: DDL Generation & Schema Creation

**Purpose**: Generate production-ready DDL statements from table objects, validate them, and execute them on a local database.

**Steps**:
1. **DDL Compilation (6.1)**: Generate CREATE TABLE statements from relational schema and data types (deterministic)
2. **DDL Validation (6.2)**: Validate DDL syntax and semantics using SQLite (deterministic)
3. **Schema Creation (6.3)**: Execute DDL statements to create database schema in a local SQLite database (deterministic)

**Output**:
- Complete DDL statements
- Validated SQL schema
- Database schema file (SQLite database)

**Key Logic**:
- Deterministic DDL generation from normalized schema
- SQLite-based validation for syntax and semantic checks
- Automatic database schema creation in local SQLite database
- Database file created in run directory (or temp directory if run directory not available)
- All steps are deterministic - no LLM interaction required

### Phase 7: Information Mining

**Purpose**: Extract information needs from natural language description and validate SQL queries (read-only, no schema modification).

**Steps**:
1. **Information Need Identification (7.1)**: Extract query requirements from NL description (iterative loop until no new needs)
2. **SQL Generation and Validation (7.2)**: Generate and validate SQL queries for each information need (batch processing)

**Output**:
- List of information needs/queries
- Validated SQL query templates
- Query metadata and validation results

**Key Logic**:
- Iterative information need extraction
- SQL query generation and validation
- No schema modification (read-only phase)
- Batch processing for efficiency

### Phase 8: Functional Dependencies & Constraints

**Purpose**: Analyze functional dependencies, identify categorical columns, and detect comprehensive constraints.

**Steps**:
1. **Functional Dependency Analysis (8.1)**: Extract functional dependencies from NL description and schema
2. **Categorical Column Identification (8.2)**: Identify categorical/enum columns (batch processing)
3. **Categorical Value Identification (8.3)**: Extract possible values for categorical columns (batch processing)
4. **Constraint Detection (8.4)**: Extract all constraints from NL (iterative loop until no changes)
5. **Constraint Scope Analysis (8.5)**: Determine scope of each constraint (batch processing, parallel)
6. **Constraint Enforcement Strategy (8.6)**: Determine how to enforce constraints (batch processing, parallel)
7. **Constraint Conflict Detection (8.7)**: Detect conflicting constraints (iterative loop if conflicts found)
8. **Constraint Compilation (8.8)**: Compile constraint specifications into final format

**Output**:
- Functional dependency list
- Categorical column definitions with values
- Complete constraint specifications (statistical, distribution, other)
- Enforcement strategies
- Conflict resolutions

**Key Logic**:
- FD extraction from both explicit mentions and implicit patterns
- Batch processing for categorical identification
- Iterative constraint detection until convergence
- Parallel constraint analysis for efficiency
- Conflict detection and resolution
- Constraint categorization (statistical, distribution, other)

### Phase 9: Constraints & Generation Strategies

**Purpose**: Define generation strategies for independent attributes (excludes derived and constrained columns).

**Steps**:
1. **Numerical Range Definition (9.1)**: Define ranges and distributions for numerical attributes
2. **Text Generation Strategy (9.2)**: Define strategies for text/string attributes
3. **Boolean Dependency Analysis (9.3)**: Analyze dependencies for boolean attributes
4. **Data Volume Specifications (9.4)**: Specify data volumes and row counts per table
5. **Partitioning Strategy (9.5)**: Define partitioning strategies for large tables
6. **Distribution Compilation (9.6)**: Compile complete distribution specifications

**Output**:
- Generation strategies per independent attribute
- Distribution parameters (ranges, patterns, etc.)
- Data volume specifications
- Partitioning strategies
- Complete data generation plan

**Key Logic**:
- Focuses only on independent attributes (excludes derived and constrained columns)
- Strategy selection based on attribute type and requirements
- Parameter extraction from NL descriptions
- Distribution pattern recognition (Zipf, log-normal, seasonal, etc.)
- Constraint-aware generation planning

## Technology Stack

### Backend
- **Python 3.13+**: Core language
- **FastAPI**: Web framework and REST API
- **LangChain 0.3.27**: LLM integration framework
- **LangGraph 1.0.1**: Workflow orchestration
- **Pydantic 2.0+**: Data validation and models
- **OpenAI API**: LLM provider (GPT-4o-mini, configurable)
- **WebSockets**: Real-time communication
- **LangSmith**: Observability and tracing

### Frontend
- **React 18**: UI framework
- **TypeScript**: Type-safe JavaScript
- **Material-UI (MUI)**: Component library
- **Zustand**: State management
- **React Hook Form**: Form handling
- **Zod**: Schema validation
- **Axios**: HTTP client
- **Vite**: Build tool

### Pipeline
- **LangGraph 1.0.1**: State machine orchestration with checkpointing
- **LangChain 0.3.27**: LLM integration framework
- **Custom DSL**: Domain-specific language for derived attributes (Lark parser)
- **Sentence Transformers**: Semantic similarity for entity consolidation
- **Lark 1.1.9+**: DSL parsing and validation
- **Pydantic 2.0+**: Data validation and models
- **LangSmith**: Observability, tracing, and debugging

## Project Structure

```
Project v4/
├── backend/                           # FastAPI backend
│   ├── api/                          # API routes
│   │   ├── routes/                   # REST endpoints
│   │   │   ├── processing.py         # Pipeline execution endpoints
│   │   │   ├── schema.py             # Schema retrieval endpoints
│   │   │   ├── suggestions.py        # Suggestion endpoints
│   │   │   └── checkpoints.py        # Checkpoint management
│   │   └── websocket.py              # WebSocket handler
│   ├── services/                     # Business logic
│   │   ├── nl2data_service.py        # Pipeline integration
│   │   ├── validation_service.py     # Schema validation
│   │   ├── conversion_service.py     # Format conversion
│   │   ├── diagram_service.py        # ER diagram generation
│   │   ├── er_diagram_compiler.py    # ER diagram compilation
│   │   ├── suggestion_service.py     # AI-powered suggestions
│   │   └── status_ticker_service.py  # Status updates
│   ├── models/                       # Pydantic models
│   │   ├── requests.py               # Request models
│   │   ├── responses.py              # Response models
│   │   └── websocket_events.py        # WebSocket event models
│   ├── utils/                        # Utilities
│   │   ├── job_manager.py            # Job tracking
│   │   ├── llm_client.py              # LLM client wrapper
│   │   └── websocket_manager.py      # WebSocket management
│   ├── static/                       # Static files
│   │   └── er_diagrams/              # Generated ER diagrams
│   ├── tests/                        # Backend tests
│   ├── config.py                     # Configuration
│   ├── dependencies.py               # Dependency injection
│   └── main.py                       # FastAPI app entry
│
├── front end/                        # React frontend
│   ├── src/
│   │   ├── components/              # React components
│   │   │   ├── checkpoints/         # Checkpoint management UI
│   │   │   ├── generation/          # Data generation UI
│   │   │   ├── layout/              # Layout components
│   │   │   ├── nl-input/            # Natural language input
│   │   │   ├── progress/            # Progress indicators
│   │   │   └── schema/              # Schema editors
│   │   ├── hooks/                   # React hooks
│   │   │   ├── useAutoSave.ts       # Auto-save functionality
│   │   │   ├── useSuggestions.ts    # Suggestion hooks
│   │   │   └── useWebSocket.ts      # WebSocket hooks
│   │   ├── services/                # API clients
│   │   │   ├── apiService.ts        # REST API client
│   │   │   ├── websocketService.ts  # WebSocket client
│   │   │   └── qualityCalculator.ts # Quality metrics
│   │   ├── stores/                  # Zustand stores
│   │   │   └── useAppStore.ts       # Main application store
│   │   ├── types/                   # TypeScript types
│   │   │   ├── api.ts               # API types
│   │   │   ├── schema.ts            # Schema types
│   │   │   ├── state.ts             # State types
│   │   │   └── websocket.ts         # WebSocket types
│   │   ├── utils/                   # Utilities
│   │   │   ├── constants.ts         # Constants
│   │   │   ├── helpers.ts           # Helper functions
│   │   │   └── validation.ts       # Validation utilities
│   │   ├── App.tsx                  # Main app component
│   │   └── main.tsx                 # Entry point
│   ├── package.json                  # Node dependencies
│   ├── tsconfig.json                # TypeScript config
│   └── vite.config.ts               # Vite config
│
├── NL2DATA/                          # Core pipeline
│   ├── orchestration/                # LangGraph orchestration
│   │   ├── graphs/                  # Phase graphs
│   │   │   ├── master.py            # Master workflow graph
│   │   │   ├── phase1.py           # Phase 1 graph
│   │   │   ├── phase2.py            # Phase 2 graph
│   │   │   ├── phase3.py            # Phase 3 graph
│   │   │   ├── phase4.py            # Phase 4 graph
│   │   │   ├── phase5.py            # Phase 5 graph
│   │   │   ├── phase6.py            # Phase 6 graph
│   │   │   ├── phase7.py            # Phase 7 graph
│   │   │   ├── phase8.py            # Phase 8 graph
│   │   │   ├── phase9.py            # Phase 9 graph
│   │   │   └── common.py           # Common utilities
│   │   ├── phase_gates/             # Phase validation gates
│   │   ├── state.py                 # State management (IRGenerationState)
│   │   └── step_registry/           # Step definitions registry
│   ├── phases/                      # Phase implementations
│   │   ├── phase1/                 # Domain & Entity Discovery
│   │   │   ├── step_1_1_domain_detection.py
│   │   │   ├── step_1_2_entity_mention_detection.py
│   │   │   ├── step_1_4_key_entity_extraction.py
│   │   │   ├── step_1_5_relation_mention_detection.py
│   │   │   ├── step_1_6_auxiliary_entity_suggestion.py
│   │   │   ├── step_1_7_entity_consolidation.py
│   │   │   ├── step_1_75_entity_relation_reclassification.py
│   │   │   ├── step_1_8_entity_cardinality.py
│   │   │   ├── step_1_9_key_relations_extraction.py
│   │   │   ├── step_1_10_schema_connectivity.py
│   │   │   ├── step_1_11_relation_cardinality.py
│   │   │   └── step_1_12_relation_validation.py
│   │   ├── phase2/                  # Attribute Discovery
│   │   ├── phase3/                  # ER Design Compilation
│   │   ├── phase4/                  # Relational Schema Compilation
│   │   ├── phase5/                  # Data Type Assignment
│   │   ├── phase6/                  # DDL Generation
│   │   ├── phase7/                  # Information Mining
│   │   ├── phase8/                  # Functional Dependencies & Constraints
│   │   ├── phase9/                  # Generation Strategies
│   │   └── phase10/                 # Model router utilities
│   ├── ir/                          # Intermediate Representation
│   │   ├── compilation/            # IR compilation utilities
│   │   ├── models/                 # IR data models
│   │   └── state_utils.py          # State utility functions
│   ├── utils/                       # Utilities
│   │   ├── llm/                     # LLM integration
│   │   │   ├── chain_utils.py       # LangChain utilities
│   │   │   ├── json_schema_fix.py   # JSON schema fixes
│   │   │   └── model_router.py      # Model routing
│   │   ├── dsl/                     # DSL parser/validator
│   │   ├── tools/                   # Validation tools
│   │   ├── validation/              # Validation utilities
│   │   ├── similarity/              # Semantic similarity
│   │   ├── fd/                      # Functional dependency utilities
│   │   ├── nl/                      # Natural language processing
│   │   ├── logging/                 # Logging setup
│   │   ├── observability/           # LangSmith integration
│   │   ├── rate_limiting/           # Rate limiting
│   │   ├── error_handling/          # Error handling
│   │   ├── loops/                   # Loop execution utilities
│   │   ├── context_manager/         # Context management
│   │   ├── cost_tracking/           # Cost tracking
│   │   ├── data_types/              # Data type utilities
│   │   ├── distributions/           # Distribution utilities
│   │   └── env/                     # Environment utilities
│   ├── config/                      # Configuration
│   │   ├── config.yaml              # Main configuration file
│   │   └── loader.py                # Config loader
│   ├── runs/                        # Execution runs (checkpoints)
│   ├── logs/                         # Log files
│   └── tests/                        # Test suite
│
├── requirements.txt                  # Python dependencies
├── nl_descriptions.txt               # Example NL descriptions
└── README.md                         # This file
```

## Installation & Setup

### Prerequisites
- Python 3.13+
- Node.js 18+
- OpenAI API key

### Backend Setup

1. **Install Python dependencies**:
```bash
pip install -r requirements.txt
```

2. **Set environment variables**:
```bash
# Required
export OPENAI_API_KEY="your-api-key-here"

# Optional - for observability and tracing
export LANGCHAIN_API_KEY="your-langsmith-key"
export LANGCHAIN_PROJECT="nl2data-project"  # LangSmith project name
export LANGCHAIN_TRACING_V2="true"  # Enable LangSmith tracing

# Optional - for debugging
export PYTHONUNBUFFERED="1"  # Unbuffered output for immediate logs
```

**Windows (PowerShell)**:
```powershell
$env:OPENAI_API_KEY="your-api-key-here"
$env:LANGCHAIN_API_KEY="your-langsmith-key"
```

**Windows (CMD)**:
```cmd
set OPENAI_API_KEY=your-api-key-here
set LANGCHAIN_API_KEY=your-langsmith-key
```

Alternatively, create a `.env` file in the project root:
```
OPENAI_API_KEY=your-api-key-here
LANGCHAIN_API_KEY=your-langsmith-key
LANGCHAIN_PROJECT=nl2data-project
LANGCHAIN_TRACING_V2=true
```

3. **Run backend server**:
```bash
cd backend
python main.py
```

The backend will start on `http://localhost:8000`

### Frontend Setup

1. **Install Node dependencies**:
```bash
cd "front end"
npm install
```

2. **Start development server**:
```bash
npm run dev
```

The frontend will start on `http://localhost:5173`

### Configuration

Edit `NL2DATA/config/config.yaml` to customize:

**OpenAI Configuration**:
- Model selection per task type (default: gpt-4o-mini for all)
- Temperature settings (default: 0 for determinism)
- Max tokens per task type (ranges from 4000 to 16000)
- Timeout per task type (ranges from 120 to 300 seconds)

**LangChain Configuration**:
- Max retries for failed LLM calls (default: 3)
- Retry delay with exponential backoff (default: 1.0s)
- Response caching (default: enabled)
- Streaming support (default: enabled)

**Phase Configuration**:
- Max iterations for iterative refinement loops (default: 10)
- Checkpoint interval (default: 1, saves after every step)

**Rate Limiting**:
- Requests per minute (default: 500)
- Tokens per minute (default: 1,000,000)
- Max concurrent requests (default: 10)
- Per-step-type concurrency limits

**Logging**:
- Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Log format (simple or detailed)
- File logging (enabled by default)
- Log file path

**Validation**:
- Strict mode (default: enabled)
- SQL validation (default: enabled)
- IR structure validation (default: enabled)

**Cost Tracking**:
- Cost tracking (default: enabled)
- Budget limit (optional, null = no limit)

## Usage

### Web Interface

1. Open the frontend in your browser (typically `http://localhost:5173`)
2. Enter a natural language description of your database requirements
3. Click "Generate Schema"
4. Monitor real-time progress via WebSocket updates
5. Review and edit schema at checkpoints (domain, entities, relations, attributes, etc.)
6. View generated ER diagram and relational schema
7. Edit schema components as needed
8. Export DDL statements

### API Usage

#### Processing Endpoints

**Start Processing**:
```bash
curl -X POST http://localhost:8000/api/process/start \
  -H "Content-Type: application/json" \
  -d '{
    "nl_description": "Create an e-commerce database with customers, products, orders, and order items."
  }'
```

Response:
```json
{
  "job_id": "uuid-here",
  "status": "started",
  "created_at": "2024-01-01T12:00:00Z"
}
```

**Get Status**:
```bash
curl http://localhost:8000/api/process/status/{job_id}
```

Response:
```json
{
  "job_id": "uuid-here",
  "status": "running",
  "phase": 3,
  "current_step": "3.1",
  "progress": 0.45
}
```

#### Checkpoint Endpoints

The system uses checkpoint-based execution, allowing users to review and edit results at key stages:

**Get Checkpoint Data**:
```bash
curl http://localhost:8000/api/checkpoint/{job_id}/{checkpoint_type}
```

Available checkpoint types:
- `domain`: After domain detection
- `entities`: After entity extraction
- `relations`: After relation extraction
- `attributes`: After attribute discovery
- `primary_keys`: After primary key identification
- `multivalued_derived`: After multivalued/derived detection
- `nullability`: After nullability constraints
- `er_diagram`: After ER design compilation
- `datatypes`: After data type assignment
- `relational_schema`: After relational schema compilation
- `information_mining`: After information need identification
- `functional_dependencies`: After FD analysis
- `constraints`: After constraint detection
- `generation_strategies`: After generation strategy definition

**Edit Checkpoint Data**:
```bash
curl -X POST http://localhost:8000/api/checkpoint/{job_id}/{checkpoint_type}/edit \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "e-commerce",
    "has_explicit_domain": true
  }'
```

**Proceed from Checkpoint**:
```bash
curl -X POST http://localhost:8000/api/checkpoint/{job_id}/proceed \
  -H "Content-Type: application/json" \
  -d '{
    "checkpoint_type": "domain",
    "proceed_to": "entities"
  }'
```

#### Schema Endpoints

**Get Schema**:
```bash
curl http://localhost:8000/api/schema/{job_id}
```

**Get ER Diagram**:
```bash
curl http://localhost:8000/api/schema/{job_id}/er-diagram
```

**Get Distribution Metadata**:
```bash
curl http://localhost:8000/api/schema/distributions/metadata
```

**Save Schema Changes**:
```bash
curl -X POST http://localhost:8000/api/schema/{job_id}/save \
  -H "Content-Type: application/json" \
  -d '{
    "changes": { ... }
  }'
```

#### Suggestions Endpoint

**Get Suggestions**:
```bash
curl -X POST http://localhost:8000/api/suggestions \
  -H "Content-Type: application/json" \
  -d '{
    "nl_description": "Create a database...",
    "context": "I need help with..."
  }'
```

#### WebSocket Connection

Real-time progress updates are delivered via WebSocket:

```javascript
const ws = new WebSocket(`ws://localhost:8000/ws/connect/${job_id}`);

ws.onmessage = (event) => {
  const update = JSON.parse(event.data);
  
  switch (update.type) {
    case 'progress':
      console.log('Progress:', update.data);
      break;
    case 'step_complete':
      console.log('Step completed:', update.data);
      break;
    case 'phase_complete':
      console.log('Phase completed:', update.data);
      break;
    case 'checkpoint':
      console.log('Checkpoint reached:', update.data);
      break;
    case 'error':
      console.error('Error:', update.data);
      break;
    case 'complete':
      console.log('Pipeline completed:', update.data);
      break;
  }
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('WebSocket closed');
};
```

**WebSocket Event Types**:
- `progress`: Step-by-step progress updates
- `step_complete`: Individual step completion
- `phase_complete`: Phase completion
- `checkpoint`: Checkpoint reached (user can review/edit)
- `error`: Error occurred during processing
- `complete`: Pipeline execution completed

### Programmatic Usage

**Complete Pipeline Execution**:
```python
from NL2DATA.orchestration.graphs.master import create_complete_workflow_graph
from NL2DATA.orchestration.state import create_initial_state

# Create workflow
workflow = create_complete_workflow_graph()

# Initialize state
nl_description = "Create a database for tracking IoT sensor readings..."
state = create_initial_state(nl_description)

# Execute pipeline
final_state = await workflow.ainvoke(state)

# Access results
entities = final_state.get("entities", [])
ddl_statements = final_state.get("ddl_statements", [])
relational_schema = final_state.get("metadata", {}).get("relational_schema", {})
generation_strategies = final_state.get("generation_strategies", {})
```

**Execute Up to Specific Phase**:
```python
from NL2DATA.orchestration.graphs.master import create_workflow_up_to_phase

# Create workflow up to Phase 5
workflow = create_workflow_up_to_phase(max_phase=5)

# Execute
state = create_initial_state(nl_description)
result = await workflow.ainvoke(state)
```

**Execute Single Phase**:
```python
from NL2DATA.orchestration.graphs.master import get_phase_graph

# Get Phase 1 graph
phase_1_graph = get_phase_graph(phase=1)

# Execute Phase 1 only
state = create_initial_state(nl_description)
result = await phase_1_graph.ainvoke(state)
```

**With Checkpointing**:
```python
from langgraph.checkpoint.memory import MemorySaver
from NL2DATA.orchestration.graphs.master import create_complete_workflow_graph

# Create workflow with checkpointing
checkpointer = MemorySaver()
workflow = create_complete_workflow_graph()  # Already includes MemorySaver

# Execute with config for checkpointing
config = {"configurable": {"thread_id": "run-1"}}
state = create_initial_state(nl_description)

# Execute pipeline (checkpoints saved automatically)
final_state = await workflow.ainvoke(state, config=config)

# Restore from checkpoint
from langgraph.graph import START
checkpoint = checkpointer.get(config)
if checkpoint:
    # Resume from checkpoint
    pass
```

## Key Features

### 1. Intelligent Entity Extraction
- Semantic understanding of entity mentions
- Automatic entity consolidation using similarity matching
- Support for auxiliary entity suggestion
- Entity-relation reclassification

### 2. Advanced Attribute Discovery
- Intrinsic attribute extraction
- Synonym detection and merging
- Composite attribute decomposition with DSL
- Derived attribute formula extraction
- Cross-entity attribute reconciliation

### 3. DSL for Derived Attributes
Custom domain-specific language for expressing derived attributes and composite decompositions:

**Derived Attribute Examples**:
```
distance_miles = distance_km * 0.621371
gross_fare = distance_km * base_fare_per_km * surge_multiplier + booking_fee
fraud_risk_score = base_risk_score + 0.5 * is_cross_border + 0.7 * high_risk_mcc_flag
total_price = quantity * unit_price * (1 - discount_rate)
age_years = (current_date - birth_date) / 365.25
```

**DSL Features**:
- Arithmetic operations: `+`, `-`, `*`, `/`, `%`
- Comparison operations: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical operations: `and`, `or`, `not`
- Conditional expressions: `if condition then value1 else value2`
- Function calls: `sqrt(x)`, `abs(x)`, `round(x, decimals)`
- Attribute references: `Entity.attribute_name`
- Constants and literals: numbers, strings, booleans

**DSL Validation**:
- Syntax validation using Lark parser
- Semantic validation (attribute existence, type checking)
- Dependency cycle detection
- Error reporting with line numbers and context

### 4. Functional Dependency Analysis
- Automatic FD extraction from NL descriptions
- 3NF normalization
- Dependency preservation

### 5. Constraint Detection
- Comprehensive constraint extraction
- Conflict detection and resolution
- Enforcement strategy selection

### 6. Data Generation Planning
- Distribution pattern recognition (Zipf, log-normal, seasonal)
- Temporal pattern detection
- Relationship-aware generation strategies

### 7. Real-time Progress Tracking
- WebSocket-based live updates
- Step-by-step progress indicators
- Error reporting

### 8. Validation & Error Correction
- Multi-level validation (syntax, semantics, constraints)
- Automatic error correction loops
- Comprehensive error reporting
- Phase gates for state validation
- Schema freeze after Phase 4 (no structural changes)

### 9. Checkpoint-Based Execution
- Review and edit results at key stages
- Step-by-step pipeline execution
- User-controlled progression
- State persistence and restoration

### 10. Real-time Observability
- LangSmith integration for tracing
- Comprehensive logging at multiple levels
- Cost tracking and budget management
- Performance metrics and timing

## Implementation Details

### State Management

The pipeline uses a centralized `IRGenerationState` TypedDict that accumulates information across all phases:

```python
class IRGenerationState(TypedDict, total=False):
    # Input
    nl_description: str  # Original natural language description
    
    # Phase tracking
    phase: int  # Current phase (1-9)
    current_step: str  # Current step identifier (e.g., "1.4")
    
    # Phase 1: Domain & Entity Discovery
    domain: Optional[str]  # Detected or inferred domain
    has_explicit_domain: Optional[bool]  # Whether domain was explicitly mentioned
    entities: List[Dict[str, Any]]  # List of entities
    relations: List[Dict[str, Any]]  # List of relationships
    entity_cardinalities: Dict[str, Dict[str, str]]  # entity -> cardinality info
    relation_cardinalities: Dict[str, Dict[str, Any]]  # relation_id -> cardinality info
    
    # Phase 2: Attribute Discovery & Schema Design
    attributes: Dict[str, List[Dict[str, Any]]]  # entity -> list of attributes
    primary_keys: Dict[str, List[str]]  # entity -> list of PK attribute names
    foreign_keys: List[Dict[str, Any]]  # List of foreign key definitions
    constraints: Annotated[List[Dict[str, Any]], add]  # Accumulated constraints
    derived_formulas: Dict[str, Dict[str, Any]]  # "Entity.attribute" -> formula DSL
    derived_metrics: Dict[str, Dict[str, Any]]  # Derived metric definitions
    composite_decompositions: Dict[str, Any]  # Composite attribute decompositions
    
    # Phase 3: Query Requirements & Schema Refinement
    information_needs: Annotated[List[Dict[str, Any]], add]  # Accumulated information needs
    sql_queries: Annotated[List[Dict[str, Any]], add]  # Accumulated SQL queries
    junction_table_names: Dict[str, str]  # relation_key -> suggested table name
    
    # Phase 4: Functional Dependencies & Data Types
    functional_dependencies: Annotated[List[Dict[str, Any]], add]  # Accumulated FDs
    data_types: Dict[str, Dict[str, Dict[str, Any]]]  # entity -> attribute -> type info
    categorical_attributes: Dict[str, List[str]]  # entity -> list of categorical attrs
    
    # Phase 5: DDL & SQL Generation
    ddl_statements: Annotated[List[str], add]  # Accumulated DDL statements
    ddl_validation_errors: List[Dict[str, Any]]  # DDL validation errors
    
    # Phase 6: Constraints & Distributions
    constraint_specs: Annotated[List[Dict[str, Any]], add]  # Accumulated constraint specs
    
    # Phase 7: Generation Strategies
    generation_strategies: Dict[str, Dict[str, Dict[str, Any]]]  # entity -> attribute -> strategy
    
    # Metadata & Tracking
    errors: Annotated[List[Dict[str, Any]], add]  # Accumulated errors
    warnings: Annotated[List[str], add]  # Accumulated warnings
    previous_answers: Dict[str, Any]  # Answers from previous steps (for context)
    metadata: Annotated[Dict[str, Any], or_]  # Flexible metadata storage (merged with dict union)
    
    # Loop tracking
    loop_iterations: Dict[str, int]  # step_id -> iteration count
    loop_termination_reasons: Dict[str, str]  # step_id -> termination reason
```

**Key State Features**:
- **Incremental Building**: Fields are optional (`total=False`) to allow incremental state building
- **Automatic Merging**: Fields marked with `Annotated[List, add]` are automatically merged when multiple nodes update them
- **Metadata Merging**: The `metadata` field uses `Annotated[Dict, or_]` to merge dictionaries using union
- **Phase Gates**: State is validated at phase boundaries using phase gates
- **Schema Freeze**: After Phase 4, the schema structure is frozen (no structural changes allowed)
- **Checkpointing**: State can be saved and restored using LangGraph checkpoints

### LangGraph Orchestration

Each phase is a compiled LangGraph StateGraph:

```python
def create_phase_1_graph() -> StateGraph:
    workflow = StateGraph(IRGenerationState)
    
    # Add nodes
    workflow.add_node("domain_detection", step_1_1)
    workflow.add_node("entity_mention", step_1_2)
    # ...
    
    # Add edges
    workflow.add_edge("domain_detection", "entity_mention")
    
    # Conditional edges
    workflow.add_conditional_edges(
        "entity_mention",
        should_infer_domain,
        {"infer": "domain_inference", "skip": "entity_extraction"}
    )
    
    return workflow.compile(checkpointer=MemorySaver())
```

### Model Routing

The system uses intelligent model routing based on task complexity:

- **Simple tasks**: gpt-4o-mini (cost-effective)
- **Important tasks**: gpt-4o-mini
- **Critical reasoning**: gpt-4o-mini (configurable to o3-pro)
- **Advanced reasoning**: gpt-4o-mini (configurable to o3)

Model selection is configured in `NL2DATA/config/config.yaml`.

### Parallel Execution

Independent operations are executed in parallel:

- Per-entity operations (cardinality, attributes)
- Per-relation operations (cardinality, validation)
- Constraint analysis
- Information need processing

### Iterative Refinement

Several steps use iterative loops until convergence:

- Schema connectivity (until all entities connected)
- Relation validation (until all valid)
- Naming validation (until all names valid)
- Constraint detection (until no new constraints)
- DDL error correction (until no errors)

### Error Handling

- Retry mechanisms with exponential backoff
- Graceful degradation
- Comprehensive error logging
- Error feedback loops for correction

### Rate Limiting

Configurable rate limiting to prevent API overload:
- Requests per minute: 500
- Tokens per minute: 1,000,000
- Max concurrent requests: 10
- Per-step-type concurrency limits:
  - Per-entity operations: 5 concurrent
  - Per-relation operations: 3 concurrent
  - Per-attribute operations: 10 concurrent
  - Per-information operations: 5 concurrent

### Checkpointing System

The pipeline uses a checkpoint-based execution model that allows users to review and edit results at key stages:

**Checkpoint Types**:
1. **Domain Checkpoint**: After domain detection (Step 1.1)
2. **Entities Checkpoint**: After entity extraction (Step 1.4)
3. **Relations Checkpoint**: After relation extraction (Step 1.9)
4. **Attributes Checkpoint**: After attribute discovery (Step 2.2)
5. **Primary Keys Checkpoint**: After primary key identification (Step 2.7)
6. **Multivalued/Derived Checkpoint**: After multivalued/derived detection (Step 2.8)
7. **Nullability Checkpoint**: After nullability constraints (Step 2.11)
8. **ER Diagram Checkpoint**: After ER design compilation (Step 3.1)
9. **Data Types Checkpoint**: After data type assignment (Phase 5)
10. **Relational Schema Checkpoint**: After relational schema compilation (Step 4.1)
11. **Information Mining Checkpoint**: After information need identification (Step 7.1)
12. **Functional Dependencies Checkpoint**: After FD analysis (Step 8.1)
13. **Constraints Checkpoint**: After constraint detection (Step 8.4)
14. **Generation Strategies Checkpoint**: After generation strategy definition (Phase 9)

**Checkpoint Workflow**:
1. Pipeline executes to checkpoint
2. State is saved and checkpoint data is extracted
3. User receives checkpoint notification via WebSocket
4. User can review and edit checkpoint data via API
5. User proceeds to next checkpoint or continues pipeline
6. Pipeline resumes from checkpoint with any edits applied

**State Persistence**:
- LangGraph MemorySaver for checkpoint storage
- State can be restored from any checkpoint
- Supports resuming interrupted executions
- Enables iterative refinement workflows

### Phase Gates

Each phase includes validation gates that ensure state integrity:

**Phase Gate Checks**:
- Required fields are present
- Data structures are valid
- No duplicate entity/relation names
- Referential integrity (foreign keys reference valid entities)
- Schema consistency
- Type correctness

**Gate Failures**:
- Phase gates raise exceptions if validation fails
- Errors are logged with detailed information
- Pipeline execution stops at failed gate
- User can review errors and fix issues before proceeding

### Schema Freeze

After Phase 4 (Relational Schema Compilation), the schema structure is frozen:
- No new entities can be added
- No new relations can be added
- No structural changes to existing entities/relations
- Only data types, constraints, and generation strategies can be modified
- Ensures consistency across later phases

## Testing

### Running Tests

```bash
# Run all tests
cd NL2DATA
python -m pytest tests/ -v

# Run specific phase tests
python -m pytest tests/phase1/ -v
python -m pytest tests/phase2/ -v

# Run with coverage
python -m pytest tests/ --cov=NL2DATA --cov-report=html

# Run specific test file
python -m pytest tests/phase1/test_step_1_1.py -v

# Run tests matching pattern
python -m pytest tests/ -k "test_entity" -v
```

### Test Structure

- **Unit tests**: Individual step testing (`tests/phase*/test_step_*.py`)
- **Phase tests**: Complete phase execution (`tests/phase*/test_phase_*.py`)
- **Integration tests**: End-to-end pipeline testing (`tests/integration/`)
- **Stress tests**: Performance and scalability testing (`tests/stress/`)
- **Backend tests**: API and service tests (`backend/tests/`)

### Example Test

```python
import pytest
from NL2DATA.orchestration.graphs.phase1 import create_phase_1_graph
from NL2DATA.orchestration.state import create_initial_state

@pytest.mark.asyncio
async def test_phase_1_integration():
    nl_description = "Create a database for IoT sensors..."
    state = create_initial_state(nl_description)
    
    phase_1_graph = create_phase_1_graph()
    result = await phase_1_graph.ainvoke(state)
    
    assert len(result["entities"]) > 0
    assert len(result["relations"]) > 0
    assert result.get("domain") is not None
```

## Troubleshooting

### Common Issues

**1. OpenAI API Errors**:
- **Issue**: `Rate limit exceeded` or `Invalid API key`
- **Solution**: 
  - Check your API key is set correctly: `echo $OPENAI_API_KEY`
  - Verify API key has sufficient credits
  - Adjust rate limiting in `config.yaml` if needed
  - Check rate limits in OpenAI dashboard

**2. Import Errors**:
- **Issue**: `ModuleNotFoundError: No module named 'NL2DATA'`
- **Solution**: 
  - Ensure you're in the project root directory
  - Install dependencies: `pip install -r requirements.txt`
  - Use Python 3.13+ (check with `python --version`)
  - Activate virtual environment if using one

**3. WebSocket Connection Issues**:
- **Issue**: WebSocket fails to connect or disconnects frequently
- **Solution**:
  - Check backend is running on port 8000
  - Verify CORS settings in `backend/config.py`
  - Check firewall settings
  - Review WebSocket logs in backend console

**4. Phase Gate Failures**:
- **Issue**: `Phase X gate failed: ...`
- **Solution**:
  - Review error message for specific validation failure
  - Check state at previous phase for missing/invalid data
  - Review logs for detailed error information
  - May need to adjust NL description or fix state manually

**5. Memory Issues**:
- **Issue**: Out of memory errors during execution
- **Solution**:
  - Reduce max concurrent requests in `config.yaml`
  - Process smaller NL descriptions
  - Increase system memory or use swap
  - Check for memory leaks in logs

**6. Checkpoint Not Found**:
- **Issue**: `Checkpoint not found` error
- **Solution**:
  - Verify job_id is correct
  - Check checkpoint was actually created (review logs)
  - Ensure checkpoint type matches available checkpoints
  - Check checkpoint storage (MemorySaver) is working

### Debugging Tips

**Enable Debug Logging**:
```yaml
# NL2DATA/config/config.yaml
logging:
  level: "DEBUG"
  format: "detailed"
```

**Check LangSmith Traces**:
- Enable LangSmith tracing with `LANGCHAIN_TRACING_V2=true`
- View traces in LangSmith dashboard
- Analyze LLM calls and responses
- Identify bottlenecks and errors

**Review Logs**:
- Backend logs: Console output and `NL2DATA/logs/nl2data.log`
- Frontend logs: Browser console (F12)
- WebSocket logs: Backend console output

**Test Individual Steps**:
```python
from NL2DATA.phases.phase1.step_1_1_domain_detection import step_1_1_domain_detection

result = await step_1_1_domain_detection("Create an e-commerce database...")
print(result)
```

## Distribution Catalog

The system includes a comprehensive distribution catalog for data generation strategies:

**Available Distributions**:
- **Uniform**: Uniform distribution between min and max
- **Normal**: Normal (Gaussian) distribution
- **Log-normal**: Log-normal distribution
- **Exponential**: Exponential distribution
- **Poisson**: Poisson distribution
- **Zipf**: Zipf distribution (power law)
- **Beta**: Beta distribution
- **Gamma**: Gamma distribution
- **Weibull**: Weibull distribution
- **Categorical**: Discrete categorical distribution
- **Seasonal**: Seasonal patterns with time-based variations
- **Trend**: Linear or polynomial trends
- **Custom**: User-defined distributions

**Distribution Parameters**:
Each distribution has specific parameters (e.g., mean, stddev for Normal; shape, scale for Gamma).
The frontend dynamically generates parameter input forms based on selected distribution.

**Accessing Distribution Metadata**:
```bash
curl http://localhost:8000/api/schema/distributions/metadata
```

## Contributing

This project follows a modular architecture. When adding new features:

1. **Keep modules focused**: Each step should have a single responsibility
2. **Use the step registry**: Register new steps in `orchestration/step_registry/registry.py`
3. **Follow the state pattern**: Update `IRGenerationState` for new data
4. **Add tests**: Include unit and integration tests
5. **Update documentation**: Keep this README and code comments up to date
6. **Follow naming conventions**: 
   - Step files: `step_{phase}_{step_number}_{description}.py`
   - Graph files: `phase{number}.py`
   - Use descriptive function and variable names
7. **Add phase gates**: Ensure new phases include appropriate validation gates
8. **Handle errors gracefully**: Use proper error handling and logging
9. **Update configuration**: Add new config options to `config.yaml` if needed
10. **Maintain backward compatibility**: Avoid breaking changes to state structure

## License

[Specify your license here]

## Acknowledgments

- Built with LangChain and LangGraph
- Uses OpenAI's GPT models
- Material-UI for frontend components
