import type { ExtractedItems, KeywordSuggestion, ValidationError } from './api';
import type { Scope } from './websocket';

export interface AppState {
  // NL Input
  nlDescription: string;
  qualityScore: number;
  qualityBreakdown: QualityBreakdown;
  extractedItems: ExtractedItems | null;
  
  // Suggestions
  keywordSuggestions: KeywordSuggestion[];
  
  // Processing
  processing: boolean;
  jobId: string | null;
  currentPhase: number | null;
  currentStep: string | null;
  progress: number;
  
  // Status Ticker
  statusTrail: StatusTick[];
  latestStatusMessage: string | null;
  statusTrailExpanded: boolean;
  
  // Results
  domain: string | null;
  entities: Entity[];
  relations: Relation[];
  erDiagram: ERDiagram | null;
  relationalSchema: RelationalSchema | null;
  
  // Editing State
  erEditing: boolean;
  schemaEditing: boolean;
  hasUnsavedERChanges: boolean;
  hasUnsavedSchemaChanges: boolean;
  schemaEditButtonEnabled: boolean;
  
  // Undo Stack
  undoStack: AppState[];
  maxUndoStackSize: number;
  
  // Data Generation
  toyData: Record<string, ToyDataTable>;
  toyDataConfig: ToyDataConfig;
}

export interface QualityBreakdown {
  domain: number;
  entities: number;
  column_names: number;
  cardinalities: number;
  constraints: number;
  relationships: number;
}

export interface StatusTick {
  job_id: string;
  seq: number;
  ts: string;
  phase: number;
  step: string;
  step_name?: string;
  scope?: Scope;
  message: string;
  level: "info" | "warning" | "error";
  summary?: Record<string, any>;
}

export interface Entity {
  name: string;
  description: string;
  attributes?: string[];
}

export interface Relation {
  entities: string[];
  type: string;
  description: string;
}

export interface ERDiagram {
  imageUrl?: string;
  entities?: Entity[];
  relations?: Relation[];
}

export interface RelationalSchema {
  tables: Table[];
}

export interface Table {
  name: string;
  columns: Column[];
  primaryKey?: string[];
  foreignKeys?: ForeignKey[];
}

export interface Column {
  name: string;
  type: string;
  nullable?: boolean;
  default?: any;
}

export interface ForeignKey {
  from: string[];
  to: string;
  toColumns: string[];
}

export interface ToyDataTable {
  tableName: string;
  rows: number;
  columns: string[];
  data: Record<string, any>[];
}

export interface ToyDataConfig {
  defaultRowsPerTable: number;
  maxRowsPerTable: number;
  rowsPerTable: Record<string, number>;
  validationErrors: ValidationError[];
  cardinalityValid: boolean;
}

