// Request types
export interface ProcessStartRequest {
  nl_description: string;
}

export interface SuggestionsRequest {
  nl_description: string;
}

export interface SaveChangesRequest {
  job_id: string;
  edit_mode: "er_diagram" | "relational_schema";
  changes: Record<string, any>;
}

export interface ToyDataGenerationRequest {
  job_id: string;
  config: {
    default_rows_per_table: number;
    max_rows_per_table: number;
    rows_per_table: Record<string, number>;
  };
  state: Record<string, any>;
}

export interface CSVGenerationRequest {
  job_id: string;
  config: {
    rows_per_table: Record<string, number>;
  };
  state: Record<string, any>;
}

// Response types
export interface ProcessStartResponse {
  job_id: string;
  status: string;
  created_at: string;
}

export interface KeywordSuggestion {
  text: string;
  type: "domain" | "entity" | "constraint" | "attribute" | "relationship" | "distribution";
  enhanced_nl_description: string;
}

export interface ExtractedItems {
  domain: string | null;
  entities: string[];
  cardinalities: string[];
  column_names: string[];
  constraints: string[];
  relationships: string[];
}

export interface SuggestionsResponse {
  keywords: KeywordSuggestion[];
  extracted_items: ExtractedItems;
}

export interface ValidationError {
  type: string;
  entity?: string;
  attribute?: string;
  message: string;
  fix_suggestion: string;
}

export interface SaveChangesResponse {
  status: "success" | "validation_failed";
  updated_state?: Record<string, any>;
  validation_errors: ValidationError[];
  er_diagram_image_url?: string;
}

export interface DistributionParameter {
  name: string;
  type: "decimal" | "integer" | "array" | "string";
  description?: string;
}

export interface DistributionMetadata {
  name: string;
  parameters: DistributionParameter[];
}

export interface DistributionMetadataResponse {
  distributions: DistributionMetadata[];
}

// Checkpoint request types
export interface DomainEditRequest {
  job_id: string;
  domain: string;
}

export interface EntitiesEditRequest {
  job_id: string;
  entities: Array<{
    name: string;
    description: string;
    mention_type?: "explicit" | "implied";
    evidence?: string;
    confidence?: number;
    reasoning?: string;
    cardinality?: string;
    table_type?: string;
  }>;
}

export interface RelationsEditRequest {
  job_id: string;
  relations: Array<{
    entities: string[];
    type: string;
    description?: string;
    cardinalities?: Record<string, any>;
    participations?: Record<string, any>;
  }>;
}

export interface AttributesEditRequest {
  job_id: string;
  attributes: Record<string, Array<{
    name: string;
    description?: string;
    data_type?: string;
    is_primary_key?: boolean;
    is_foreign_key?: boolean;
  }>>;
}

export interface PrimaryKeysEditRequest {
  job_id: string;
  primary_keys: Record<string, string[]>;
}

export interface ERDiagramEditRequest {
  job_id: string;
  er_design: {
    entities: Array<{
      name: string;
      description: string;
      attributes: any[];
      primary_key: string[];
    }>;
    relations: Array<{
      entities: string[];
      type: string;
      description: string;
    }>;
    attributes: Record<string, any[]>;
  };
}

export interface DatatypesEditRequest {
  job_id: string;
  data_types: Record<string, any>;  // entity -> attribute_types
}

export interface MultivaluedDerivedEditRequest {
  job_id: string;
  multivalued_derived: Record<string, any>;
  derived_formulas: Record<string, any>;
}

export interface NullabilityEditRequest {
  job_id: string;
  nullability: Record<string, any>;
}

export interface RelationalSchemaEditRequest {
  job_id: string;
  relational_schema: {
    tables: Array<{
      name: string;
      columns: Array<{
        name: string;
        description?: string;
        type_hint?: string;
        is_primary_key?: boolean;
      }>;
      primary_key: string[];
      foreign_keys?: any[];
    }>;
  };
}

export interface InformationMiningEditRequest {
  job_id: string;
  information_needs: Array<{
    description: string;
    sql_query: string;
    entities_involved?: string[];
    [key: string]: any;
  }>;
}

export interface FunctionalDependenciesEditRequest {
  job_id: string;
  functional_dependencies: Array<{
    lhs: string[];  // List of attributes (determinant)
    rhs: string[];  // List of attributes (dependent)
    reasoning?: string;
    [key: string]: any;
  }>;
}

export interface ConstraintsEditRequest {
  job_id: string;
  constraints: Array<{
    description: string;
    constraint_category?: string;
    affected_components?: {
      affected_entities?: string[];
      affected_attributes?: string[];
    };
    [key: string]: any;
  }>;
}

export interface GenerationStrategiesEditRequest {
  job_id: string;
  generation_strategies: Record<string, Record<string, any>>;  // entity -> attribute -> strategy
}

export interface CheckpointProceedRequest {
  job_id: string;
}

// Checkpoint response types
export interface CheckpointResponse {
  checkpoint_type: "domain" | "entities" | "relations" | "attributes" | "primary_keys" | "multivalued_derived" | "nullability" | "er_diagram" | "datatypes" | "relational_schema" | "information_mining" | "functional_dependencies" | "constraints" | "generation_strategies" | "complete";
  data: {
    domain?: string;
    has_explicit_domain?: boolean;
    entities?: Array<{
      name: string;
      description: string;
      mention_type?: "explicit" | "implied";
      evidence?: string;
      confidence?: number;
      reasoning?: string;
      cardinality?: string;
      table_type?: string;
    }>;
    relations?: Array<{
      entities: string[];
      type: string;
      description?: string;
      cardinalities?: Record<string, any>;
      participations?: Record<string, any>;
    }>;
    relation_cardinalities?: Record<string, any>;
    attributes?: Record<string, Array<{
      name: string;
      description?: string;
      data_type?: string;
      is_primary_key?: boolean;
      is_foreign_key?: boolean;
    }>>;
    primary_keys?: Record<string, string[]>;
    er_design?: {
      entities: Array<{
        name: string;
        description: string;
        attributes: any[];
        primary_key: string[];
      }>;
      relations: Array<{
        entities: string[];
        type: string;
        description: string;
      }>;
      attributes: Record<string, any[]>;
    };
    relational_schema?: {
      tables: Array<{
        name: string;
        columns: Array<{
          name: string;
          description?: string;
          type_hint?: string;
          is_primary_key?: boolean;
        }>;
        primary_key: string[];
        foreign_keys?: any[];
      }>;
    };
    information_needs?: Array<{
      description: string;
      sql_query: string;
      entities_involved?: string[];
      [key: string]: any;
    }>;
    functional_dependencies?: Array<{
      lhs: string[];
      rhs: string[];
      reasoning?: string;
      [key: string]: any;
    }>;
    constraints?: Array<{
      description: string;
      constraint_category?: string;
      affected_components?: {
        affected_entities?: string[];
        affected_attributes?: string[];
      };
      [key: string]: any;
    }>;
    generation_strategies?: Record<string, Record<string, any>>;
    justification?: Record<string, any>;
    message?: string;
  };
  justification?: Record<string, any>;
}

export interface CheckpointProceedResponse {
  status: "success" | "error";
  message: string;
  next_checkpoint?: string | null;
  checkpoint_data?: Record<string, any>;
  checkpoint_justification?: Record<string, any> | null;
}

