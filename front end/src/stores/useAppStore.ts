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
        // WebSocket connection will be established by useWebSocket hook
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

