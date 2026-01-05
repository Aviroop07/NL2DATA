import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import type { 
  AppState, 
  KeywordSuggestion, 
  ExtractedItems, 
  StatusTick,
  Entity,
  Relation,
  ERDiagram,
  RelationalSchema,
  ToyDataTable,
  ToyDataConfig
} from '../types/state';
import { apiService } from '../services/apiService';
import { calculateQualityScore } from '../services/qualityCalculator';
import { MAX_STATUS_TRAIL_SIZE, MAX_UNDO_STACK_SIZE } from '../utils/constants';

interface AppStore extends AppState {
  // NL Input Actions
  setNLDescription: (description: string) => void;
  setKeywordSuggestions: (suggestions: KeywordSuggestion[]) => void;
  setExtractedItems: (items: ExtractedItems) => void;
  submitProcessing: (nlDescription: string) => Promise<void>;
  
  // Status Ticker Actions
  addStatusTick: (tick: StatusTick) => void;
  setLatestStatusMessage: (message: string) => void;
  setStatusTrailExpanded: (expanded: boolean) => void;
  
  // Results Actions
  setDomain: (domain: string) => void;
  setEntities: (entities: Entity[]) => void;
  setRelations: (relations: Relation[]) => void;
  setERDiagram: (diagram: ERDiagram | null) => void;
  setRelationalSchema: (schema: RelationalSchema | null) => void;
  
  // Checkpoint Actions
  fetchCheckpoint: (jobId: string, retryCount?: number, maxRetries?: number) => Promise<void>;
  saveDomainEdit: (jobId: string, domain: string) => Promise<void>;
  saveEntitiesEdit: (jobId: string, entities: Entity[]) => Promise<void>;
  saveRelationsEdit: (jobId: string, relations: Relation[]) => Promise<void>;
  saveAttributesEdit: (jobId: string, attributes: Record<string, any[]>) => Promise<void>;
  savePrimaryKeysEdit: (jobId: string, primaryKeys: Record<string, string[]>) => Promise<void>;
  saveMultivaluedDerivedEdit: (jobId: string, multivaluedDerived: Record<string, any>, derivedFormulas: Record<string, any>) => Promise<void>;
  saveNullabilityEdit: (jobId: string, nullability: Record<string, any>) => Promise<void>;
  saveERDiagramEdit: (jobId: string, erDesign: Record<string, any>) => Promise<void>;
  saveDatatypesEdit: (jobId: string, dataTypes: Record<string, any>) => Promise<void>;
  saveRelationalSchemaEdit: (jobId: string, relationalSchema: Record<string, any>) => Promise<void>;
  saveInformationMiningEdit: (jobId: string, informationNeeds: Array<Record<string, any>>) => Promise<void>;
  saveFunctionalDependenciesEdit: (jobId: string, functionalDependencies: Array<Record<string, any>>) => Promise<void>;
  saveConstraintsEdit: (jobId: string, constraints: Array<Record<string, any>>) => Promise<void>;
  saveGenerationStrategiesEdit: (jobId: string, generationStrategies: Record<string, Record<string, any>>) => Promise<void>;
  proceedToNextCheckpoint: (jobId: string) => Promise<void>;
  markCheckpointCompleted: (type: "domain" | "entities" | "relations" | "attributes" | "primary_keys" | "multivalued_derived" | "er_diagram" | "relational_schema" | "datatypes" | "nullability" | "information_mining" | "functional_dependencies" | "constraints" | "generation_strategies", data: Record<string, any>) => void;
  
  // Editing State Machine Actions
  setEREditing: (editing: boolean) => void;
  setSchemaEditing: (editing: boolean) => void;
  setHasUnsavedERChanges: (hasChanges: boolean) => void;
  setHasUnsavedSchemaChanges: (hasChanges: boolean) => void;
  saveERChanges: (changes?: Record<string, any>) => Promise<void>;
  discardERChanges: () => void;
  saveSchemaChanges: (changes?: Record<string, any>) => Promise<void>;
  discardSchemaChanges: () => void;
  
  // Undo Stack Actions
  undo: () => void;
  canUndo: () => boolean;
  
  // Data Generation Actions
  setToyData: (data: Record<string, ToyDataTable>) => void;
  setToyDataConfig: (config: ToyDataConfig) => void;
}

export const useAppStore = create<AppStore>()(
  immer((set, get) => ({
    // Initial state
    nlDescription: '',
    qualityScore: 0,
    qualityBreakdown: {
      domain: 0,
      entities: 0,
      column_names: 0,
      cardinalities: 0,
      constraints: 0,
      relationships: 0
    },
    extractedItems: null,
    keywordSuggestions: [],
    processing: false,
    jobId: null,
    currentPhase: null,
    currentStep: null,
    progress: 0,
    statusTrail: [],
    latestStatusMessage: null,
    statusTrailExpanded: false,
    domain: null,
    entities: [],
    relations: [],
    erDiagram: null,
    relationalSchema: null,
    erEditing: false,
    schemaEditing: false,
    hasUnsavedERChanges: false,
    hasUnsavedSchemaChanges: false,
    schemaEditButtonEnabled: false,
    undoStack: [],
    maxUndoStackSize: MAX_UNDO_STACK_SIZE,
    toyData: {},
    toyDataConfig: {
      defaultRowsPerTable: 10,
      maxRowsPerTable: 100,
      rowsPerTable: {},
      validationErrors: [],
      cardinalityValid: false
    },
    currentCheckpoint: null,
    checkpointData: null,
    checkpointJustification: null,
    completedCheckpoints: [],
    
    // Actions
    setNLDescription: (description) => {
      set((state) => {
        state.nlDescription = description;
      });
    },
    
    setKeywordSuggestions: (suggestions) => {
      set((state) => {
        state.keywordSuggestions = suggestions;
      });
    },
    
    setExtractedItems: (items) => {
      set((state) => {
        state.extractedItems = items;
        // Recalculate quality score
        const { score, breakdown } = calculateQualityScore(items);
        state.qualityScore = score;
        state.qualityBreakdown = breakdown;
      });
    },
    
    addStatusTick: (tick) => {
      set((state) => {
        state.statusTrail.push(tick);
        state.latestStatusMessage = tick.message;
        // Keep only last N items
        if (state.statusTrail.length > MAX_STATUS_TRAIL_SIZE) {
          state.statusTrail.shift();
        }
      });
    },
    
    setLatestStatusMessage: (message) => {
      set((state) => {
        state.latestStatusMessage = message;
      });
    },
    
    submitProcessing: async (nlDescription) => {
      console.log('submitProcessing called with:', nlDescription.substring(0, 50) + '...');
      
      set((state) => {
        state.processing = true;
        state.jobId = null;
        state.statusTrail = [];
        state.latestStatusMessage = "Starting processing...";
        // Clear suggestions when processing starts
        state.keywordSuggestions = [];
      });
      
      try {
        console.log('Calling apiService.startProcessing...');
        const response = await apiService.startProcessing({ nl_description: nlDescription });
        console.log('Processing started, job_id:', response.job_id);
        
        set((state) => {
          state.jobId = response.job_id;
          state.latestStatusMessage = `Processing started (Job ID: ${response.job_id.substring(0, 8)}...)`;
        });
        
        // Fetch initial checkpoint (domain) - direct API call, no polling
        const { fetchCheckpoint } = get();
        await fetchCheckpoint(response.job_id);
      } catch (error: any) {
        console.error('Failed to start processing:', error);
        console.error('Error details:', {
          message: error?.message,
          response: error?.response?.data,
          status: error?.response?.status
        });
        
        set((state) => {
          state.processing = false;
          state.latestStatusMessage = `Error: ${error?.response?.data?.detail || error?.message || 'Failed to start processing'}`;
        });
        
        // Show error to user
        const errorMessage = error?.response?.data?.detail || error?.message || 'Unknown error';
        alert(`Failed to start processing: ${errorMessage}`);
      }
    },
    
    // Status Ticker Actions
    setStatusTrailExpanded: (expanded) => {
      set((state) => {
        state.statusTrailExpanded = expanded;
      });
    },
    
    // Results Actions
    setDomain: (domain) => {
      set((state) => {
        state.domain = domain;
      });
    },
    
    setEntities: (entities) => {
      set((state) => {
        state.entities = entities;
      });
    },
    
    setRelations: (relations) => {
      set((state) => {
        state.relations = relations;
      });
    },
    
    setERDiagram: (diagram) => {
      set((state) => {
        state.erDiagram = diagram;
      });
    },
    
    setRelationalSchema: (schema) => {
      set((state) => {
        state.relationalSchema = schema;
      });
    },
    
    // Checkpoint Actions
    fetchCheckpoint: async (jobId, retryCount = 0, maxRetries = 30) => {
      try {
        const checkpoint = await apiService.getCheckpoint(jobId);
        set((state) => {
          // Only mark previous checkpoint as completed if we're explicitly fetching a NEW checkpoint
          // This prevents automatic progression when fetchCheckpoint is called for the same checkpoint
          if (state.currentCheckpoint && 
              state.currentCheckpoint !== checkpoint.checkpoint_type &&
              checkpoint.checkpoint_type !== "complete") {
            // Check if already in completed list
            const alreadyCompleted = state.completedCheckpoints.some(
              c => c.type === state.currentCheckpoint
            );
            // Only mark as completed if the previous checkpoint was explicitly completed via proceedToNextCheckpoint
            // Don't auto-complete when just fetching checkpoint data
            // The proceedToNextCheckpoint function already handles marking checkpoints as completed
            // So we skip auto-completion here to prevent premature progression
          }
          
          state.currentCheckpoint = checkpoint.checkpoint_type;
          state.checkpointData = checkpoint.data;
          state.checkpointJustification = checkpoint.justification || null;
          
          // Update domain, entities, relations based on checkpoint
          if (checkpoint.checkpoint_type === "domain" && checkpoint.data.domain) {
            state.domain = checkpoint.data.domain;
          }
          if (checkpoint.checkpoint_type === "entities" && checkpoint.data.entities) {
            state.entities = checkpoint.data.entities.map((e: any) => ({
              name: e.name,
              description: e.description || "",
              attributes: []
            }));
          }
          if (checkpoint.checkpoint_type === "relations" && checkpoint.data.relations) {
            state.relations = checkpoint.data.relations.map((r: any) => ({
              entities: r.entities || [],
              type: r.type || "",
              description: r.description || "",
              entity_cardinalities: r.entity_cardinalities || {},
              entity_participations: r.entity_participations || {}
            }));
          }
          if (checkpoint.checkpoint_type === "primary_keys" && checkpoint.data.primary_keys) {
            // Primary keys are already in the right format
          }
          if (checkpoint.checkpoint_type === "er_diagram" && checkpoint.data.er_design) {
            const erDesign = checkpoint.data.er_design;
            state.erDiagram = {
              entities: erDesign.entities || [],
              relations: erDesign.relations || [],
              imageUrl: erDesign.imageUrl || null
            };
          }
          if (checkpoint.checkpoint_type === "relational_schema" && checkpoint.data.relational_schema) {
            const schema = checkpoint.data.relational_schema;
            state.relationalSchema = {
              tables: schema.tables || []
            };
          }
        });
      } catch (error: any) {
        // If checkpoint is not ready yet (202 status), retry with exponential backoff
        if (error?.response?.status === 202 && retryCount < maxRetries) {
          const delay = Math.min(1000 * Math.pow(1.5, retryCount), 5000); // Max 5 seconds
          console.log(`Checkpoint not ready yet, retrying in ${delay}ms... (attempt ${retryCount + 1}/${maxRetries})`);
          await new Promise(resolve => setTimeout(resolve, delay));
          return get().fetchCheckpoint(jobId, retryCount + 1, maxRetries);
        }
        console.error('Failed to fetch checkpoint:', error);
        throw error;
      }
    },
    
    saveDomainEdit: async (jobId, domain) => {
      try {
        await apiService.saveDomainEdit({ job_id: jobId, domain });
        set((state) => {
          state.domain = domain;
        });
      } catch (error: any) {
        console.error('Failed to save domain edit:', error);
        throw error;
      }
    },
    
    saveEntitiesEdit: async (jobId, entities) => {
      try {
        const entitiesData = entities.map(e => ({
          name: e.name,
          description: e.description,
          mention_type: "explicit" as const,
          evidence: "",
          confidence: 1.0
        }));
        await apiService.saveEntitiesEdit({ job_id: jobId, entities: entitiesData });
        set((state) => {
          state.entities = entities;
        });
      } catch (error: any) {
        console.error('Failed to save entities edit:', error);
        throw error;
      }
    },
    
    saveRelationsEdit: async (jobId, relations) => {
      try {
        const relationsData = relations.map(r => ({
          entities: r.entities,
          type: r.type,
          description: r.description,
          entity_cardinalities: r.entity_cardinalities || {},
          entity_participations: r.entity_participations || {}
        }));
        await apiService.saveRelationsEdit({ job_id: jobId, relations: relationsData });
        set((state) => {
          state.relations = relations;
          // Update checkpointData to reflect saved state (so hasChanges resets)
          if (state.currentCheckpoint === "relations") {
            state.checkpointData = {
              ...state.checkpointData,
              relations: relationsData
            };
          }
        });
      } catch (error: any) {
        console.error('Failed to save relations edit:', error);
        throw error;
      }
    },
    
    saveAttributesEdit: async (jobId, attributes) => {
      try {
        await apiService.saveAttributesEdit({ job_id: jobId, attributes });
        set((state) => {
          // Mark attributes as completed
          if (state.currentCheckpoint === "attributes") {
            const completed = {
              type: "attributes" as const,
              data: { ...state.checkpointData, attributes },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "attributes");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save attributes edit:', error);
        throw error;
      }
    },
    
    savePrimaryKeysEdit: async (jobId, primaryKeys) => {
      try {
        await apiService.savePrimaryKeysEdit({ job_id: jobId, primary_keys: primaryKeys });
        set((state) => {
          // Mark primary keys as completed
          if (state.currentCheckpoint === "primary_keys") {
            const completed = {
              type: "primary_keys" as const,
              data: { ...state.checkpointData, primary_keys: primaryKeys },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "primary_keys");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save primary keys edit:', error);
        throw error;
      }
    },
    
    saveMultivaluedDerivedEdit: async (jobId, multivaluedDerived, derivedFormulas) => {
      try {
        await apiService.saveMultivaluedDerivedEdit({ 
          job_id: jobId, 
          multivalued_derived: multivaluedDerived,
          derived_formulas: derivedFormulas
        });
        set((state) => {
          // Update checkpointData to reflect saved state (so hasChanges resets)
          if (state.currentCheckpoint === "multivalued_derived") {
            state.checkpointData = {
              ...state.checkpointData,
              multivalued_derived: multivaluedDerived,
              derived_formulas: derivedFormulas
            };
            const completed = {
              type: "multivalued_derived" as const,
              data: { ...state.checkpointData },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "multivalued_derived");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save multivalued/derived edit:', error);
        throw error;
      }
    },
    
    saveNullabilityEdit: async (jobId, nullability) => {
      try {
        await apiService.saveNullabilityEdit({ job_id: jobId, nullability: nullability });
        set((state) => {
          if (state.currentCheckpoint === "nullability") {
            const completed = {
              type: "nullability" as const,
              data: { ...state.checkpointData, nullability: nullability },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "nullability");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save nullability edit:', error);
        throw error;
      }
    },
    
    saveDefaultValuesEdit: async (jobId, defaultValues) => {
      try {
        await apiService.saveDefaultValuesEdit({ job_id: jobId, default_values: defaultValues });
        set((state) => {
          // Update checkpointData to reflect saved state (so hasChanges resets)
          if (state.currentCheckpoint === "default_values") {
            state.checkpointData = {
              ...state.checkpointData,
              default_values: defaultValues
            };
            const completed = {
              type: "default_values" as const,
              data: { ...state.checkpointData },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "default_values");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save default values edit:', error);
        throw error;
      }
    },
    
    saveCheckConstraintsEdit: async (jobId, checkConstraints) => {
      try {
        await apiService.saveCheckConstraintsEdit({ job_id: jobId, check_constraints: checkConstraints });
        set((state) => {
          // Update checkpointData to reflect saved state (so hasChanges resets)
          if (state.currentCheckpoint === "check_constraints") {
            state.checkpointData = {
              ...state.checkpointData,
              check_constraints: checkConstraints
            };
            const completed = {
              type: "check_constraints" as const,
              data: { ...state.checkpointData },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "check_constraints");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save check constraints edit:', error);
        throw error;
      }
    },
    
    savePhase2FinalEdit: async (jobId, attributes, relationAttributes) => {
      try {
        await apiService.savePhase2FinalEdit({ 
          job_id: jobId, 
          attributes: attributes,
          relation_attributes: relationAttributes
        });
        set((state) => {
          if (state.currentCheckpoint === "phase2_final") {
            const completed = {
              type: "phase2_final" as const,
              data: { ...state.checkpointData, attributes: attributes, relation_attributes: relationAttributes },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "phase2_final");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save phase2_final edit:', error);
        throw error;
      }
    },
    
    saveERDiagramEdit: async (jobId, erDesign) => {
      try {
        await apiService.saveERDiagramEdit({ job_id: jobId, er_design: erDesign });
        set((state) => {
          // Mark ER diagram as completed
          if (state.currentCheckpoint === "er_diagram") {
            const completed = {
              type: "er_diagram" as const,
              data: { ...state.checkpointData, er_design: erDesign },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "er_diagram");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save ER diagram edit:', error);
        throw error;
      }
    },
    
    saveDatatypesEdit: async (jobId, dataTypes) => {
      try {
        await apiService.saveDatatypesEdit({ job_id: jobId, data_types: dataTypes });
        set((state) => {
          // Mark datatypes as completed
          if (state.currentCheckpoint === "datatypes") {
            const completed = {
              type: "datatypes" as const,
              data: { ...state.checkpointData, data_types: dataTypes },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "datatypes");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save datatypes edit:', error);
        throw error;
      }
    },
    
    saveRelationalSchemaEdit: async (jobId, relationalSchema) => {
      try {
        await apiService.saveRelationalSchemaEdit({ job_id: jobId, relational_schema: relationalSchema });
        set((state) => {
          // Mark relational schema as completed
          if (state.currentCheckpoint === "relational_schema") {
            const completed = {
              type: "relational_schema" as const,
              data: { ...state.checkpointData, relational_schema: relationalSchema },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "relational_schema");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
          }
        });
      } catch (error: any) {
        console.error('Failed to save relational schema edit:', error);
        throw error;
      }
    },
    
    saveInformationMiningEdit: async (jobId, informationNeeds) => {
      try {
        await apiService.saveInformationMiningEdit({ job_id: jobId, information_needs: informationNeeds });
        set((state) => {
          if (state.currentCheckpoint === "information_mining") {
            const completed = {
              type: "information_mining" as const,
              data: { ...state.checkpointData, information_needs: informationNeeds },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "information_mining");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
            // Update checkpointData to reflect saved state
            state.checkpointData = {
              ...state.checkpointData,
              information_needs: informationNeeds
            };
          }
        });
      } catch (error: any) {
        console.error('Failed to save information mining edit:', error);
        throw error;
      }
    },
    
    saveFunctionalDependenciesEdit: async (jobId, functionalDependencies) => {
      try {
        await apiService.saveFunctionalDependenciesEdit({ job_id: jobId, functional_dependencies: functionalDependencies });
        set((state) => {
          if (state.currentCheckpoint === "functional_dependencies") {
            const completed = {
              type: "functional_dependencies" as const,
              data: { ...state.checkpointData, functional_dependencies: functionalDependencies },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "functional_dependencies");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
            // Update checkpointData to reflect saved state
            state.checkpointData = {
              ...state.checkpointData,
              functional_dependencies: functionalDependencies
            };
          }
        });
      } catch (error: any) {
        console.error('Failed to save functional dependencies edit:', error);
        throw error;
      }
    },
    
    saveConstraintsEdit: async (jobId, constraints) => {
      try {
        await apiService.saveConstraintsEdit({ job_id: jobId, constraints: constraints });
        set((state) => {
          if (state.currentCheckpoint === "constraints") {
            const completed = {
              type: "constraints" as const,
              data: { ...state.checkpointData, constraints: constraints },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "constraints");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
            // Update checkpointData to reflect saved state
            state.checkpointData = {
              ...state.checkpointData,
              constraints: constraints
            };
          }
        });
      } catch (error: any) {
        console.error('Failed to save constraints edit:', error);
        throw error;
      }
    },
    
    saveGenerationStrategiesEdit: async (jobId, generationStrategies) => {
      try {
        await apiService.saveGenerationStrategiesEdit({ job_id: jobId, generation_strategies: generationStrategies });
        set((state) => {
          if (state.currentCheckpoint === "generation_strategies") {
            const completed = {
              type: "generation_strategies" as const,
              data: { ...state.checkpointData, generation_strategies: generationStrategies },
              justification: state.checkpointJustification
            };
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === "generation_strategies");
            if (!alreadyCompleted) {
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
            // Update checkpointData to reflect saved state
            state.checkpointData = {
              ...state.checkpointData,
              generation_strategies: generationStrategies
            };
          }
        });
      } catch (error: any) {
        console.error('Failed to save generation strategies edit:', error);
        throw error;
      }
    },
    
    proceedToNextCheckpoint: async (jobId) => {
      // Store current checkpoint info before clearing (for error recovery)
      let previousCheckpoint: string | null = null;
      let previousData: Record<string, any> | null = null;
      let previousJustification: Record<string, any> | null = null;
      
      try {
        // Immediately mark current checkpoint as completed and clear it
        // This makes the editable card disappear and shows the read-only version
        set((state) => {
          if (state.currentCheckpoint) {
            previousCheckpoint = state.currentCheckpoint;
            previousData = state.checkpointData;
            previousJustification = state.checkpointJustification;
            
            const currentType = state.currentCheckpoint;
            const alreadyCompleted = state.completedCheckpoints.some(c => c.type === currentType);
            if (!alreadyCompleted) {
              // For er_diagram checkpoint, ensure imageUrl is preserved
              let checkpointDataToSave = state.checkpointData || {};
              if (currentType === "er_diagram" && checkpointDataToSave.er_design) {
                // Preserve imageUrl from erDiagram state if not in checkpointData
                const imageUrlFromStore = state.erDiagram?.imageUrl;
                const imageUrlFromData = checkpointDataToSave.er_design?.imageUrl;
                if (!imageUrlFromData && imageUrlFromStore) {
                  checkpointDataToSave = {
                    ...checkpointDataToSave,
                    er_design: {
                      ...checkpointDataToSave.er_design,
                      imageUrl: imageUrlFromStore
                    }
                  };
                }
              }
              const completed = {
                type: currentType,
                data: checkpointDataToSave,
                justification: state.checkpointJustification
              };
              state.completedCheckpoints = [...state.completedCheckpoints, completed];
            }
            // Clear current checkpoint temporarily (will be set when response arrives)
            state.currentCheckpoint = null;
            state.checkpointData = null;
            state.checkpointJustification = null;
          }
        });
        
        const response = await apiService.proceedToNextCheckpoint({ job_id: jobId });
        if (response.status === "success") {
          // Handle completion
          if (response.next_checkpoint === "complete" || !response.next_checkpoint) {
            set((state) => {
              // Mark current checkpoint as completed
              if (state.currentCheckpoint) {
                const alreadyCompleted = state.completedCheckpoints.some(
                  c => c.type === state.currentCheckpoint
                );
                if (!alreadyCompleted) {
                  const completed = {
                    type: state.currentCheckpoint,
                    data: state.checkpointData || {},
                    justification: state.checkpointJustification
                  };
                  state.completedCheckpoints = [...state.completedCheckpoints, completed];
                }
              }
              state.currentCheckpoint = "complete";
              state.checkpointData = response.checkpoint_data || { message: "Pipeline completed successfully" };
              state.checkpointJustification = null;
              state.processing = false;
            });
            return;
          }
          
          // No polling needed - checkpoint data is in the response
          if (response.next_checkpoint && response.checkpoint_data) {
            console.log(`Proceeding to checkpoint: ${response.next_checkpoint}`, response.checkpoint_data);
            set((state) => {
              // Update checkpoint data directly from response
              state.currentCheckpoint = response.next_checkpoint as any;
              state.checkpointData = response.checkpoint_data;
              state.checkpointJustification = response.checkpoint_justification || null;
              
              // Update specific state based on checkpoint type
              if (response.next_checkpoint === "domain" && response.checkpoint_data.domain) {
                state.domain = response.checkpoint_data.domain;
              }
              if (response.next_checkpoint === "entities" && response.checkpoint_data.entities) {
                state.entities = response.checkpoint_data.entities.map((e: any) => ({
                  name: e.name,
                  description: e.description || "",
                  attributes: []
                }));
              }
              if (response.next_checkpoint === "relations" && response.checkpoint_data.relations) {
                state.relations = response.checkpoint_data.relations.map((r: any) => ({
                  entities: r.entities || [],
                  type: r.type || "",
                  description: r.description || "",
                  entity_cardinalities: r.entity_cardinalities || {},
                  entity_participations: r.entity_participations || {}
                }));
              }
              if (response.next_checkpoint === "attributes" && response.checkpoint_data.attributes) {
                // Attributes are already in the right format
              }
              if (response.next_checkpoint === "primary_keys" && response.checkpoint_data.primary_keys) {
                // Primary keys are already in the right format
              }
              if (response.next_checkpoint === "multivalued_derived" && response.checkpoint_data.multivalued_derived) {
                // Multivalued/derived data is already in the right format
              }
              if (response.next_checkpoint === "nullability" && response.checkpoint_data.nullability) {
                // Nullability data is already in the right format
              }
              if (response.next_checkpoint === "default_values" && response.checkpoint_data.default_values) {
                // Default values data is already in the right format
              }
              if (response.next_checkpoint === "check_constraints" && response.checkpoint_data.check_constraints) {
                // Check constraints data is already in the right format
              }
              if (response.next_checkpoint === "phase2_final" && response.checkpoint_data.attributes) {
                // Phase2 final data is already in the right format
              }
              if (response.next_checkpoint === "er_diagram" && response.checkpoint_data.er_design) {
                const erDesign = response.checkpoint_data.er_design;
                state.erDiagram = {
                  entities: erDesign.entities || [],
                  relations: erDesign.relations || [],
                  imageUrl: erDesign.imageUrl || null
                };
              }
              if (response.next_checkpoint === "datatypes" && response.checkpoint_data.data_types) {
                // Datatypes are already in the right format
              }
              if (response.next_checkpoint === "relational_schema" && response.checkpoint_data.relational_schema) {
                const schema = response.checkpoint_data.relational_schema;
                state.relationalSchema = {
                  tables: schema.tables || []
                };
              }
            });
          } else {
            // Fallback: fetch checkpoint if data not in response (shouldn't happen, but safety net)
            const { fetchCheckpoint } = get();
            await fetchCheckpoint(jobId);
          }
        } else if (response.status === "error") {
          throw new Error(response.message);
        }
      } catch (error: any) {
        console.error('Failed to proceed to next checkpoint:', error);
        // Restore previous checkpoint state on error
        if (previousCheckpoint) {
          set((state) => {
            state.currentCheckpoint = previousCheckpoint as any;
            state.checkpointData = previousData;
            state.checkpointJustification = previousJustification;
            // Remove the checkpoint we just added to completed (since it failed)
            state.completedCheckpoints = state.completedCheckpoints.filter(
              c => c.type !== previousCheckpoint
            );
          });
        }
        throw error;
      }
    },
    
    markCheckpointCompleted: (type, data) => {
      set((state) => {
        // Check if already completed
        const alreadyCompleted = state.completedCheckpoints.some(c => c.type === type);
        if (!alreadyCompleted) {
          const completed = {
            type,
            data,
            justification: state.checkpointJustification
          };
          state.completedCheckpoints = [...state.completedCheckpoints, completed];
        }
      });
    },
    
    // Editing State Machine Actions
    setEREditing: (editing) => {
      set((state) => {
        state.erEditing = editing;
        // If starting to edit ER, disable schema editing
        if (editing) {
          state.schemaEditing = false;
        }
      });
    },
    
    setSchemaEditing: (editing) => {
      set((state) => {
        // Can only edit schema if ER is saved (no unsaved ER changes)
        if (editing && state.hasUnsavedERChanges) {
          console.warn('Cannot edit schema: ER has unsaved changes');
          return;
        }
        state.schemaEditing = editing;
        // If starting to edit schema, disable ER editing
        if (editing) {
          state.erEditing = false;
        }
      });
    },
    
    setHasUnsavedERChanges: (hasChanges) => {
      set((state) => {
        state.hasUnsavedERChanges = hasChanges;
        // Update schema edit button state
        state.schemaEditButtonEnabled = !hasChanges;
      });
    },
    
    setHasUnsavedSchemaChanges: (hasChanges) => {
      set((state) => {
        state.hasUnsavedSchemaChanges = hasChanges;
      });
    },
    
    saveERChanges: async (changes = {}) => {
      const state = get();
      if (!state.jobId || !state.hasUnsavedERChanges) return;
      
      try {
        const response = await apiService.saveChanges({
          job_id: state.jobId,
          edit_mode: "er_diagram",
          changes
        });
        
        if (response.status === "success" && response.updated_state) {
          set((state) => {
            // Update state from backend
            if (response.updated_state.er_diagram) {
              state.erDiagram = response.updated_state.er_diagram;
            }
            if (response.updated_state.relational_schema) {
              state.relationalSchema = response.updated_state.relational_schema;
            }
            // Update ER diagram image URL if provided
            if (response.er_diagram_image_url) {
              if (!state.erDiagram) {
                state.erDiagram = { imageUrl: response.er_diagram_image_url };
              } else {
                state.erDiagram.imageUrl = response.er_diagram_image_url;
              }
            }
            // Clear unsaved changes
            state.hasUnsavedERChanges = false;
            state.erEditing = false;
            // Unlock schema editing
            state.schemaEditButtonEnabled = true;
          });
        } else {
          // Show validation errors
          console.error('Validation errors:', response.validation_errors);
        }
      } catch (error) {
        console.error('Failed to save ER changes:', error);
      }
    },
    
    discardERChanges: () => {
      set((state) => {
        state.hasUnsavedERChanges = false;
        state.erEditing = false;
        // Revert to last saved state (would need to load from backend or keep snapshot)
      });
    },
    
    saveSchemaChanges: async (changes = {}) => {
      const state = get();
      if (!state.jobId || !state.hasUnsavedSchemaChanges) return;
      
      try {
        const response = await apiService.saveChanges({
          job_id: state.jobId,
          edit_mode: "relational_schema",
          changes
        });
        
        if (response.status === "success" && response.updated_state) {
          set((state) => {
            if (response.updated_state.relational_schema) {
              state.relationalSchema = response.updated_state.relational_schema;
            }
            state.hasUnsavedSchemaChanges = false;
            state.schemaEditing = false;
          });
        }
      } catch (error) {
        console.error('Failed to save schema changes:', error);
      }
    },
    
    discardSchemaChanges: () => {
      set((state) => {
        state.hasUnsavedSchemaChanges = false;
        state.schemaEditing = false;
      });
    },
    
    // Undo Stack (client-side, last 5 committed changes)
    undo: () => {
      const state = get();
      if (state.undoStack.length === 0) return;
      
      const previousState = state.undoStack.pop()!;
      // Restore previous state (merge with current, preserving some UI state)
      set((state) => {
        const preservedUIState = {
          statusTrailExpanded: state.statusTrailExpanded
        };
        Object.assign(state, previousState);
        // Preserve UI state
        state.statusTrailExpanded = preservedUIState.statusTrailExpanded;
      });
    },
    
    canUndo: () => {
      return get().undoStack.length > 0;
    },
    
    // Data Generation Actions
    setToyData: (data) => {
      set((state) => {
        state.toyData = data;
      });
    },
    
    setToyDataConfig: (config) => {
      set((state) => {
        state.toyDataConfig = config;
      });
    }
  }))
);

