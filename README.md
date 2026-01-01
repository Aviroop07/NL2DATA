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
│  │ Phase 3: Query Requirements & Schema Refinement     │  │
│  │ Phase 4: Functional Dependencies & Data Types       │  │
│  │ Phase 5: DDL & SQL Generation                        │  │
│  │ Phase 6: Constraints & Distributions                │  │
│  │ Phase 7: Generation Strategies                      │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Component Details

**Frontend (`front end/`)**
- Built with React 18, TypeScript, and Material-UI
- Real-time WebSocket communication for live progress updates
- State management using Zustand
- Form validation with React Hook Form and Zod
- ER diagram and relational schema visualization/editing

**Backend (`backend/`)**
- FastAPI-based REST API
- WebSocket support for bidirectional communication
- Job management system for tracking long-running pipeline executions
- Service layer abstraction for pipeline integration
- Request/response models with Pydantic

**NL2DATA Pipeline (`NL2DATA/`)**
- LangGraph-based workflow orchestration
- Modular phase-based architecture
- State management through TypedDict (IRGenerationState)
- LLM integration with OpenAI (configurable models)
- Comprehensive validation and error handling

## Pipeline Overview

The NL2DATA pipeline consists of 7 sequential phases, each building upon the previous phase's output:

```
Phase 1: Domain & Entity Discovery
    ↓
Phase 2: Attribute Discovery & Schema Design
    ↓
Phase 3: Query Requirements & Schema Refinement
    ↓
Phase 4: Functional Dependencies & Data Types
    ↓
Phase 5: DDL & SQL Generation
    ↓
Phase 6: Constraints & Distributions
    ↓
Phase 7: Generation Strategies
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

### Phase 3: Query Requirements & Schema Refinement

**Purpose**: Extract information needs, generate SQL queries, and refine the schema based on query requirements.

**Steps**:
1. **Information Need Identification (3.1)**: Extract query requirements from NL description
2. **Information Completeness (3.2)**: Check if schema supports all information needs
3. **Phase 2 Re-execution (3.3)**: Re-run Phase 2 if schema is incomplete (conditional)
4. **ER Design Compilation (3.4)**: Compile complete ER design from Phase 1 & 2 results
5. **Junction Table Naming (3.45)**: Generate names for many-to-many junction tables
6. **Relational Schema Compilation (3.5)**: Convert ER design to relational schema

**Output**:
- List of information needs/queries
- SQL query templates
- Complete ER design
- Relational schema (tables, columns, keys)
- Junction table definitions

**Key Logic**:
- Query-driven schema refinement
- Conditional re-execution of Phase 2 if gaps found
- ER to relational mapping with junction table handling

### Phase 4: Functional Dependencies & Data Types

**Purpose**: Analyze functional dependencies, perform normalization, and assign data types.

**Steps**:
1. **Functional Dependency Analysis (4.1)**: Extract FDs from NL description and schema
2. **3NF Normalization (4.2)**: Normalize schema to 3rd Normal Form
3. **Data Type Assignment (4.3)**: Assign appropriate data types to all attributes
4. **Categorical Detection (4.4)**: Identify categorical/enum attributes
5. **Check Constraint Detection (4.5)**: Extract value constraints for categoricals
6. **Categorical Value Extraction (4.6)**: Extract possible values for categoricals
7. **Categorical Distribution (4.7)**: Determine value distributions

**Output**:
- Functional dependency list
- Normalized schema (3NF)
- Data type assignments
- Categorical attribute definitions
- Value distributions

**Key Logic**:
- FD extraction from both explicit mentions and implicit patterns
- Automatic 3NF normalization
- Intelligent data type inference
- Categorical value extraction and distribution modeling

### Phase 5: DDL & SQL Generation

**Purpose**: Generate production-ready DDL statements and validate them.

**Steps**:
1. **DDL Compilation (5.1)**: Generate CREATE TABLE statements
2. **DDL Validation (5.2)**: Validate DDL syntax and semantics
3. **DDL Error Correction (5.3)**: Fix validation errors (loop if errors found)
4. **Schema Creation (5.4)**: Optional: Create schema in database
5. **SQL Query Generation (5.5)**: Generate SQL queries for information needs

**Output**:
- Complete DDL statements
- Validated SQL schema
- SQL query templates
- Database schema (if created)

**Key Logic**:
- Template-based DDL generation
- Multi-pass validation
- Automatic error correction
- Query generation from information needs

### Phase 6: Constraints & Distributions

**Purpose**: Detect additional constraints and specify data distributions.

**Steps**:
1. **Constraint Detection (6.1)**: Extract all constraints from NL (loop until no changes)
2. **Constraint Scope Analysis (6.2)**: Determine scope of each constraint (parallel)
3. **Constraint Enforcement Strategy (6.3)**: Determine how to enforce constraints (parallel)
4. **Constraint Conflict Detection (6.4)**: Detect conflicting constraints (loop to 6.3 if conflicts)
5. **Constraint Compilation (6.5)**: Compile constraint specifications

**Output**:
- Complete constraint specifications
- Enforcement strategies
- Conflict resolutions
- Distribution patterns

**Key Logic**:
- Iterative constraint detection
- Conflict resolution
- Parallel constraint analysis
- Distribution pattern extraction (Zipf, log-normal, seasonal, etc.)

### Phase 7: Generation Strategies

**Purpose**: Define strategies for generating synthetic data that matches requirements.

**Steps**:
1. **Strategy Selection (7.1)**: Select generation strategy per attribute
2. **Distribution Parameter Extraction (7.2)**: Extract distribution parameters
3. **Temporal Pattern Detection (7.3)**: Identify temporal patterns (seasonality, trends)
4. **Relationship Constraints (7.4)**: Define inter-entity constraints for generation
5. **Validation Rules (7.5)**: Define validation rules for generated data
6. **Generation Plan (7.6)**: Compile complete generation plan

**Output**:
- Generation strategies per attribute
- Distribution parameters
- Temporal patterns
- Relationship constraints
- Complete data generation plan

**Key Logic**:
- Strategy selection based on attribute type and requirements
- Parameter extraction from NL descriptions
- Temporal pattern recognition
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
- **LangGraph**: State machine orchestration
- **Custom DSL**: Domain-specific language for derived attributes
- **Sentence Transformers**: Semantic similarity
- **Lark**: DSL parsing

## Project Structure

```
Project v4/
├── backend/                    # FastAPI backend
│   ├── api/                   # API routes
│   │   ├── routes/            # REST endpoints
│   │   └── websocket.py       # WebSocket handler
│   ├── services/              # Business logic
│   │   ├── nl2data_service.py # Pipeline integration
│   │   ├── validation_service.py
│   │   └── ...
│   ├── models/                # Pydantic models
│   ├── utils/                 # Utilities
│   └── main.py                # FastAPI app entry
│
├── front end/                 # React frontend
│   ├── src/
│   │   ├── components/       # React components
│   │   ├── services/         # API clients
│   │   ├── stores/           # Zustand stores
│   │   └── types/            # TypeScript types
│   └── package.json
│
├── NL2DATA/                   # Core pipeline
│   ├── orchestration/         # LangGraph orchestration
│   │   ├── graphs/           # Phase graphs
│   │   ├── state.py          # State management
│   │   └── step_registry/    # Step definitions
│   ├── phases/               # Phase implementations
│   │   ├── phase1/          # Domain & Entity Discovery
│   │   ├── phase2/          # Attribute Discovery
│   │   ├── phase3/          # Query Requirements
│   │   ├── phase4/          # FDs & Data Types
│   │   ├── phase5/          # DDL Generation
│   │   ├── phase6/          # Constraints
│   │   └── phase7/          # Generation Strategies
│   ├── ir/                   # Intermediate Representation
│   │   └── models/           # IR data models
│   ├── utils/                # Utilities
│   │   ├── llm/              # LLM integration
│   │   ├── dsl/              # DSL parser/validator
│   │   ├── tools/            # Validation tools
│   │   └── ...
│   ├── config/               # Configuration
│   └── tests/                # Test suite
│
├── requirements.txt          # Python dependencies
├── nl_descriptions.txt       # Example NL descriptions
└── README.md                # This file
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
export OPENAI_API_KEY="your-api-key-here"
export LANGCHAIN_API_KEY="your-langsmith-key"  # Optional, for tracing
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
- Model selection (default: gpt-4o-mini)
- Temperature settings
- Max tokens per task type
- Rate limiting
- Logging levels

## Usage

### Web Interface

1. Open the frontend in your browser
2. Enter a natural language description of your database requirements
3. Click "Generate Schema"
4. Monitor real-time progress via WebSocket updates
5. View generated ER diagram and relational schema
6. Edit schema if needed
7. Export DDL statements

### API Usage

**Start Processing**:
```bash
curl -X POST http://localhost:8000/api/process/start \
  -H "Content-Type: application/json" \
  -d '{
    "nl_description": "Create an e-commerce database with customers, products, orders, and order items."
  }'
```

**Get Status**:
```bash
curl http://localhost:8000/api/process/status/{job_id}
```

**WebSocket Connection**:
```javascript
const ws = new WebSocket('ws://localhost:8000/ws');
ws.onmessage = (event) => {
  const update = JSON.parse(event.data);
  console.log('Progress:', update);
};
```

### Programmatic Usage

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
Custom domain-specific language for expressing derived attributes:
```
distance_miles = distance_km * 0.621371
gross_fare = distance_km * base_fare_per_km * surge_multiplier + booking_fee
fraud_risk_score = base_risk_score + 0.5 * is_cross_border + 0.7 * high_risk_mcc_flag
```

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

## Implementation Details

### State Management

The pipeline uses a centralized `IRGenerationState` TypedDict that accumulates information across all phases:

```python
class IRGenerationState(TypedDict, total=False):
    nl_description: str
    phase: int
    entities: List[Dict[str, Any]]
    relations: List[Dict[str, Any]]
    attributes: Dict[str, List[Dict[str, Any]]]
    primary_keys: Dict[str, List[str]]
    foreign_keys: List[Dict[str, Any]]
    functional_dependencies: List[Dict[str, Any]]
    ddl_statements: List[str]
    # ... and more
```

State is passed between phases and updated incrementally. Fields marked with `Annotated[List, add]` are automatically merged when multiple nodes update them.

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
- Per-step-type concurrency limits

## Testing

### Running Tests

```bash
# Run all tests
cd NL2DATA
python -m pytest tests/

# Run specific phase tests
python -m pytest tests/phase1/
python -m pytest tests/phase2/

# Run integration tests
python -m pytest tests/integration_test.py

# Run stress tests
python tests/stress/run_phase1_stress.py
```

### Test Structure

- **Unit tests**: Individual step testing
- **Phase tests**: Complete phase execution
- **Integration tests**: End-to-end pipeline testing
- **Stress tests**: Performance and scalability testing

### Example Test

```python
async def test_phase_1_integration():
    nl_description = "Create a database for IoT sensors..."
    state = create_initial_state(nl_description)
    
    phase_1_graph = create_phase_1_graph()
    result = await phase_1_graph.ainvoke(state)
    
    assert len(result["entities"]) > 0
    assert len(result["relations"]) > 0
```

## Contributing

This project follows a modular architecture. When adding new features:

1. **Keep modules focused**: Each step should have a single responsibility
2. **Use the step registry**: Register new steps in `orchestration/step_registry/registry.py`
3. **Follow the state pattern**: Update `IRGenerationState` for new data
4. **Add tests**: Include unit and integration tests
5. **Update documentation**: Keep this README and code comments up to date

## License

[Specify your license here]

## Acknowledgments

- Built with LangChain and LangGraph
- Uses OpenAI's GPT models
- Material-UI for frontend components
