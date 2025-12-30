// Schema-specific types for ER Diagram and Relational Schema editing

export interface EntityFormData {
  name: string;
  description: string;
  attributes: AttributeFormData[];
}

export interface AttributeFormData {
  name: string;
  type: string;
  nullable: boolean;
  description?: string;
}

export interface RelationFormData {
  entities: string[];
  type: string;
  description: string;
  cardinalities?: Record<string, string>;
}

export interface TableFormData {
  name: string;
  columns: ColumnFormData[];
  primaryKey: string[];
  foreignKeys: ForeignKeyFormData[];
}

export interface ColumnFormData {
  name: string;
  type: string;
  nullable: boolean;
  default?: any;
  distribution?: DistributionFormData;
}

export interface ForeignKeyFormData {
  from: string[];
  to: string;
  toColumns: string[];
}

export interface DistributionFormData {
  type: string;
  parameters: Record<string, any>;
}

