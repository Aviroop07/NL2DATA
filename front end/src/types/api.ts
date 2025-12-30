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

