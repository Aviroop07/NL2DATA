import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Alert,
  CircularProgress,
  List,
  ListItem,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  ToggleButton,
  ToggleButtonGroup
} from '@mui/material';
import { Save, PlayArrow, Add, Delete, Edit } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';
import type { Relation } from '../../types/state';

const RelationsCheckpoint: React.FC = () => {
  const { 
    jobId, 
    relations, 
    entities,
    checkpointData,
    checkpointJustification,
    saveRelationsEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedRelations, setEditedRelations] = useState<Relation[]>([]);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [newRelation, setNewRelation] = useState<{
    entities: string[];
    type: string;
    description: string;
    entity_cardinalities?: Record<string, "1" | "N">;
    entity_participations?: Record<string, "total" | "partial">;
  }>({ 
    entities: [], 
    type: '', 
    description: '' 
  });
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [hasChanges, setHasChanges] = useState(false);
  const [isInitialized, setIsInitialized] = useState(false);
  
  // Track the last checkpoint data key to detect when it changes (e.g., after save)
  const [lastCheckpointDataKey, setLastCheckpointDataKey] = useState<string>('');
  
  useEffect(() => {
    // Priority: checkpointData.relations > relations prop
    const relationsToUse = checkpointData?.relations || relations || [];
    const checkpointDataKey = JSON.stringify(relationsToUse);
    
    // Initialize when we have data and haven't initialized yet
    if (relationsToUse.length > 0 && !isInitialized) {
      const normalizedRelations = relationsToUse.map((rel: any) => ({
        entities: rel.entities || [],
        type: rel.type || "",
        description: rel.description || "",
        entity_cardinalities: rel.entity_cardinalities || {},
        entity_participations: rel.entity_participations || {}
      }));
      setEditedRelations(normalizedRelations);
      setIsInitialized(true);
      setLastCheckpointDataKey(checkpointDataKey);
      setHasChanges(false); // No changes on initial load
    } else if (isInitialized && checkpointDataKey !== lastCheckpointDataKey && relationsToUse.length > 0) {
      // Checkpoint data changed (e.g., after save) - update editedRelations to match
      const normalizedRelations = relationsToUse.map((rel: any) => ({
        entities: rel.entities || [],
        type: rel.type || "",
        description: rel.description || "",
        entity_cardinalities: rel.entity_cardinalities || {},
        entity_participations: rel.entity_participations || {}
      }));
      setEditedRelations(normalizedRelations);
      setLastCheckpointDataKey(checkpointDataKey);
      setHasChanges(false); // Reset changes after save
    }
  }, [relations, checkpointData, isInitialized, lastCheckpointDataKey]);
  
  useEffect(() => {
    if (!isInitialized) {
      setHasChanges(false);
      return;
    }
    
    // Normalize both arrays for comparison (don't sort entities - order matters)
    const normalizeRelation = (rel: any) => ({
      entities: rel.entities || [],
      type: rel.type || "",
      description: rel.description || "",
      entity_cardinalities: rel.entity_cardinalities || {},
      entity_participations: rel.entity_participations || {}
    });
    
    const originalRelations = checkpointData?.relations || relations || [];
    const normalizedOriginal = originalRelations.map(normalizeRelation);
    const normalizedEdited = editedRelations.map(normalizeRelation);
    
    const relationsChanged = JSON.stringify(normalizedEdited) !== JSON.stringify(normalizedOriginal);
    setHasChanges(relationsChanged);
  }, [editedRelations, relations, checkpointData, isInitialized]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveRelationsEdit(jobId, editedRelations);
      // After saving, the backend has been updated
      // The hasChanges will be recalculated on next render when checkpointData updates
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save relations');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save relations to backend first to ensure state is updated
      const relationsData = editedRelations.map(r => ({
        entities: r.entities,
        type: r.type,
        description: r.description,
        entity_cardinalities: r.entity_cardinalities || {},
        entity_participations: r.entity_participations || {}
      }));
      
      // Save relations to backend state before proceeding
      await saveRelationsEdit(jobId, editedRelations);
      
      // Mark current checkpoint as completed before proceeding (with current data)
      markCheckpointCompleted("relations", { ...checkpointData, relations: relationsData });
      
      // proceedToNextCheckpoint will clear currentCheckpoint and show read-only version
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const handleDelete = (index: number) => {
    setEditedRelations(editedRelations.filter((_, i) => i !== index));
  };
  
  const handleEdit = (index: number) => {
    setEditingIndex(index);
  };
  
  const handleSaveEdit = (index: number, relation: Relation) => {
    const updated = [...editedRelations];
    updated[index] = relation;
    setEditedRelations(updated);
    setEditingIndex(null);
  };
  
  const handleAdd = () => {
    if (newRelation.entities.length >= 2 && newRelation.type) {
      // Use provided cardinalities/participations or initialize defaults
      const entityCardinalities: Record<string, "1" | "N"> = {};
      const entityParticipations: Record<string, "total" | "partial"> = {};
      
      newRelation.entities.forEach((entityName) => {
        entityCardinalities[entityName] = newRelation.entity_cardinalities?.[entityName] || "1";
        entityParticipations[entityName] = newRelation.entity_participations?.[entityName] || "partial";
      });
      
      setEditedRelations([...editedRelations, {
        entities: newRelation.entities,
        type: newRelation.type,
        description: newRelation.description,
        entity_cardinalities: entityCardinalities,
        entity_participations: entityParticipations
      }]);
      setNewRelation({ entities: [], type: '', description: '' });
      setAddDialogOpen(false);
    }
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  const relationTypes = ['one-to-one', 'one-to-many', 'many-to-one', 'many-to-many'];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Relations Review
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}
      
      <Paper 
        elevation={0}
        sx={{ 
          p: 3, 
          mb: 3,
          bgcolor: 'grey.50',
          border: '1px solid',
          borderColor: 'divider'
        }}
      >
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="subtitle2" sx={{ color: 'text.secondary' }}>
            Found {editedRelations.length} relations
          </Typography>
          <Button
            variant="outlined"
            size="small"
            startIcon={<Add />}
            onClick={() => setAddDialogOpen(true)}
            disabled={entityNames.length < 2}
          >
            Add Relation
          </Button>
        </Box>
        
        <List>
          {editedRelations.map((relation, index) => (
            <ListItem
              key={index}
              sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                mb: 1,
                bgcolor: 'background.paper'
              }}
            >
              {editingIndex === index ? (
                <RelationEditForm
                  relation={relation}
                  entities={entityNames}
                  relationTypes={relationTypes}
                  onSave={(rel) => handleSaveEdit(index, rel)}
                  onCancel={() => setEditingIndex(null)}
                />
              ) : (
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1 }}>
                    <Box sx={{ flex: 1 }}>
                      <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                        {relation.entities.join(' ↔ ')}
                      </Typography>
                      <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.5 }}>
                        Type: {relation.type} {relation.description ? `- ${relation.description}` : ''}
                      </Typography>
                    </Box>
                    <Box>
                      <IconButton size="small" onClick={() => handleEdit(index)}>
                        <Edit />
                      </IconButton>
                      <IconButton size="small" onClick={() => handleDelete(index)} color="error">
                        <Delete />
                      </IconButton>
                    </Box>
                  </Box>
                  
                  {/* Entity Cardinalities and Participations */}
                  {relation.entities.length > 0 && (
                    <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                      {relation.entities.map((entityName) => {
                        // Use actual values from relation, checking both the relation object and editedRelations state
                        // Priority: editedRelations[index] > relation (from props) > defaults
                        const currentRelation = editedRelations[index];
                        const cardinality = currentRelation?.entity_cardinalities?.[entityName] 
                          ?? relation.entity_cardinalities?.[entityName] 
                          ?? "1";
                        const participation = currentRelation?.entity_participations?.[entityName] 
                          ?? relation.entity_participations?.[entityName] 
                          ?? "partial";
                        
                        return (
                          <Box 
                            key={entityName}
                            sx={{ 
                              display: 'flex', 
                              alignItems: 'center', 
                              gap: 2,
                              p: 1.5,
                              bgcolor: 'grey.50',
                              borderRadius: 1,
                              border: '1px solid',
                              borderColor: 'divider'
                            }}
                          >
                            <Typography variant="body2" sx={{ minWidth: 120, fontWeight: 500 }}>
                              {entityName}:
                            </Typography>
                            
                            {/* Cardinality Toggle */}
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                                Cardinality:
                              </Typography>
                              <ToggleButtonGroup
                                value={cardinality}
                                exclusive
                                onChange={(_, newValue) => {
                                  if (newValue !== null) {
                                    const updated = [...editedRelations];
                                    updated[index] = {
                                      ...updated[index],
                                      entity_cardinalities: {
                                        ...(updated[index].entity_cardinalities || {}),
                                        [entityName]: newValue as "1" | "N"
                                      }
                                    };
                                    setEditedRelations(updated);
                                  }
                                }}
                                size="small"
                                sx={{ height: 32 }}
                              >
                                <ToggleButton 
                                  value="1" 
                                  aria-label="one"
                                  sx={{
                                    bgcolor: cardinality === "1" ? "primary.main" : "white",
                                    color: cardinality === "1" ? "white" : "text.primary",
                                    "&.Mui-selected": {
                                      bgcolor: "primary.main !important",
                                      color: "white !important"
                                    },
                                    "&:hover": {
                                      bgcolor: cardinality === "1" ? "primary.dark" : "white"
                                    },
                                    "&.Mui-selected:hover": {
                                      bgcolor: "primary.dark !important"
                                    },
                                    border: "1px solid",
                                    borderColor: "divider"
                                  }}
                                >
                                  1
                                </ToggleButton>
                                <ToggleButton 
                                  value="N" 
                                  aria-label="many"
                                  sx={{
                                    bgcolor: cardinality === "N" ? "primary.main" : "white",
                                    color: cardinality === "N" ? "white" : "text.primary",
                                    "&.Mui-selected": {
                                      bgcolor: "primary.main !important",
                                      color: "white !important"
                                    },
                                    "&:hover": {
                                      bgcolor: cardinality === "N" ? "primary.dark" : "white"
                                    },
                                    "&.Mui-selected:hover": {
                                      bgcolor: "primary.dark !important"
                                    },
                                    border: "1px solid",
                                    borderColor: "divider"
                                  }}
                                >
                                  N
                                </ToggleButton>
                              </ToggleButtonGroup>
                            </Box>
                            
                            {/* Participation Toggle */}
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                                Participation:
                              </Typography>
                              <ToggleButtonGroup
                                value={participation}
                                exclusive
                                onChange={(_, newValue) => {
                                  if (newValue !== null) {
                                    const updated = [...editedRelations];
                                    updated[index] = {
                                      ...updated[index],
                                      entity_participations: {
                                        ...(updated[index].entity_participations || {}),
                                        [entityName]: newValue as "total" | "partial"
                                      }
                                    };
                                    setEditedRelations(updated);
                                  }
                                }}
                                size="small"
                                sx={{ height: 32 }}
                              >
                                <ToggleButton 
                                  value="partial" 
                                  aria-label="partial"
                                  sx={{
                                    bgcolor: participation === "partial" ? "primary.main" : "white",
                                    color: participation === "partial" ? "white" : "text.primary",
                                    "&.Mui-selected": {
                                      bgcolor: "primary.main !important",
                                      color: "white !important"
                                    },
                                    "&:hover": {
                                      bgcolor: participation === "partial" ? "primary.dark" : "white"
                                    },
                                    "&.Mui-selected:hover": {
                                      bgcolor: "primary.dark !important"
                                    },
                                    border: "1px solid",
                                    borderColor: "divider"
                                  }}
                                >
                                  Partial
                                </ToggleButton>
                                <ToggleButton 
                                  value="total" 
                                  aria-label="total"
                                  sx={{
                                    bgcolor: participation === "total" ? "primary.main" : "white",
                                    color: participation === "total" ? "white" : "text.primary",
                                    "&.Mui-selected": {
                                      bgcolor: "primary.main !important",
                                      color: "white !important"
                                    },
                                    "&:hover": {
                                      bgcolor: participation === "total" ? "primary.dark" : "white"
                                    },
                                    "&.Mui-selected:hover": {
                                      bgcolor: "primary.dark !important"
                                    },
                                    border: "1px solid",
                                    borderColor: "divider"
                                  }}
                                >
                                  Total
                                </ToggleButton>
                              </ToggleButtonGroup>
                            </Box>
                          </Box>
                        );
                      })}
                    </Box>
                  )}
                </Box>
              )}
            </ListItem>
          ))}
        </List>
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="relations" />
          </Box>
        )}
      </Paper>
      
      <Dialog open={addDialogOpen} onClose={() => setAddDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Add New Relation</DialogTitle>
        <DialogContent>
          <FormControl fullWidth sx={{ mb: 2 }}>
            <InputLabel>Entities (select 2 or more)</InputLabel>
            <Select
              multiple
              value={newRelation.entities}
              onChange={(e) => setNewRelation({ ...newRelation, entities: e.target.value as string[] })}
              renderValue={(selected) => (selected as string[]).join(', ')}
            >
              {entityNames.map((name) => (
                <MenuItem key={name} value={name}>
                  {name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <FormControl fullWidth sx={{ mb: 2 }}>
            <InputLabel>Relation Type</InputLabel>
            <Select
              value={newRelation.type}
              onChange={(e) => setNewRelation({ ...newRelation, type: e.target.value })}
            >
              {relationTypes.map((type) => (
                <MenuItem key={type} value={type}>
                  {type}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
          <TextField
            fullWidth
            label="Description (optional)"
            multiline
            rows={3}
            value={newRelation.description}
            onChange={(e) => setNewRelation({ ...newRelation, description: e.target.value })}
            sx={{ mb: 2 }}
          />
          
          {/* Entity Cardinalities and Participations for new relation */}
          {newRelation.entities.length > 0 && (
            <NewRelationConstraints 
              entities={newRelation.entities}
              onConstraintsChange={(cardinalities, participations) => {
                // Store constraints in component state (will be used in handleAdd)
                setNewRelation(prev => ({
                  ...prev,
                  entity_cardinalities: cardinalities,
                  entity_participations: participations
                }));
              }}
            />
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddDialogOpen(false)}>Cancel</Button>
          <Button 
            onClick={handleAdd} 
            variant="contained" 
            disabled={newRelation.entities.length < 2 || !newRelation.type}
          >
            Add
          </Button>
        </DialogActions>
      </Dialog>
      
      <Box sx={{ display: 'flex', gap: 2, justifyContent: 'flex-end' }}>
        <Button
          variant="outlined"
          startIcon={saving ? <CircularProgress size={16} /> : <Save />}
          onClick={handleSave}
          disabled={saving || !hasChanges}
        >
          Save Changes
        </Button>
        <Button
          variant="contained"
          startIcon={saving ? <CircularProgress size={16} /> : <PlayArrow />}
          onClick={handleProceed}
          disabled={saving || hasChanges}
        >
          Proceed
        </Button>
      </Box>
    </Box>
  );
};

const RelationEditForm: React.FC<{
  relation: Relation;
  entities: string[];
  relationTypes: string[];
  onSave: (relation: Relation) => void;
  onCancel: () => void;
}> = ({ relation, entities, relationTypes, onSave, onCancel }) => {
  const [selectedEntities, setSelectedEntities] = useState<string[]>(relation.entities);
  const [type, setType] = useState(relation.type);
  const [description, setDescription] = useState(relation.description || '');
  const [entityCardinalities, setEntityCardinalities] = useState<Record<string, "1" | "N">>(
    relation.entity_cardinalities || {}
  );
  const [entityParticipations, setEntityParticipations] = useState<Record<string, "total" | "partial">>(
    relation.entity_participations || {}
  );
  
  // Initialize cardinalities and participations for selected entities using actual relation values
  useEffect(() => {
    const newCardinalities: Record<string, "1" | "N"> = {};
    const newParticipations: Record<string, "total" | "partial"> = {};
    
    selectedEntities.forEach((entityName) => {
      // Use actual value from relation if exists, otherwise use existing state, otherwise default
      newCardinalities[entityName] = relation.entity_cardinalities?.[entityName] ?? entityCardinalities[entityName] ?? "1";
      newParticipations[entityName] = relation.entity_participations?.[entityName] ?? entityParticipations[entityName] ?? "partial";
    });
    
    setEntityCardinalities(newCardinalities);
    setEntityParticipations(newParticipations);
  }, [selectedEntities, relation.entity_cardinalities, relation.entity_participations]);
  
  const handleSave = () => {
    onSave({
      entities: selectedEntities,
      type,
      description,
      entity_cardinalities: entityCardinalities,
      entity_participations: entityParticipations
    });
  };
  
  return (
    <Box sx={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <FormControl size="small" fullWidth>
        <InputLabel>Entities</InputLabel>
        <Select
          multiple
          value={selectedEntities}
          onChange={(e) => setSelectedEntities(e.target.value as string[])}
          renderValue={(selected) => (selected as string[]).join(', ')}
        >
          {entities.map((name) => (
            <MenuItem key={name} value={name}>
              {name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <FormControl size="small" fullWidth>
        <InputLabel>Type</InputLabel>
        <Select value={type} onChange={(e) => setType(e.target.value)}>
          {relationTypes.map((t) => (
            <MenuItem key={t} value={t}>
              {t}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
      <TextField
        size="small"
        value={description}
        onChange={(e) => setDescription(e.target.value)}
        placeholder="Description"
        fullWidth
      />
      
      {/* Entity Cardinalities and Participations */}
      {selectedEntities.length > 0 && (
        <Box sx={{ mt: 1, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'text.secondary' }}>
            Entity Constraints:
          </Typography>
          {selectedEntities.map((entityName) => (
            <Box 
              key={entityName}
              sx={{ 
                display: 'flex', 
                alignItems: 'center', 
                gap: 2,
                p: 1.5,
                bgcolor: 'grey.50',
                borderRadius: 1,
                border: '1px solid',
                borderColor: 'divider'
              }}
            >
              <Typography variant="body2" sx={{ minWidth: 100, fontWeight: 500 }}>
                {entityName}:
              </Typography>
              
              {/* Cardinality Toggle */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                  Cardinality:
                </Typography>
                <ToggleButtonGroup
                  value={entityCardinalities[entityName] ?? "1"}
                  exclusive
                  onChange={(_, newValue) => {
                    if (newValue !== null) {
                      setEntityCardinalities({
                        ...entityCardinalities,
                        [entityName]: newValue as "1" | "N"
                      });
                    }
                  }}
                  size="small"
                  sx={{ height: 32 }}
                >
                  <ToggleButton 
                    value="1" 
                    aria-label="one"
                    sx={{
                      bgcolor: (entityCardinalities[entityName] ?? "1") === "1" ? "primary.main" : "white",
                      color: (entityCardinalities[entityName] ?? "1") === "1" ? "white" : "text.primary",
                      "&.Mui-selected": {
                        bgcolor: "primary.main !important",
                        color: "white !important"
                      },
                      "&:hover": {
                        bgcolor: (entityCardinalities[entityName] ?? "1") === "1" ? "primary.dark" : "white"
                      },
                      "&.Mui-selected:hover": {
                        bgcolor: "primary.dark !important"
                      },
                      border: "1px solid",
                      borderColor: "divider"
                    }}
                  >
                    1
                  </ToggleButton>
                  <ToggleButton 
                    value="N" 
                    aria-label="many"
                    sx={{
                      bgcolor: (entityCardinalities[entityName] ?? "1") === "N" ? "primary.main" : "white",
                      color: (entityCardinalities[entityName] ?? "1") === "N" ? "white" : "text.primary",
                      "&.Mui-selected": {
                        bgcolor: "primary.main !important",
                        color: "white !important"
                      },
                      "&:hover": {
                        bgcolor: (entityCardinalities[entityName] ?? "1") === "N" ? "primary.dark" : "white"
                      },
                      "&.Mui-selected:hover": {
                        bgcolor: "primary.dark !important"
                      },
                      border: "1px solid",
                      borderColor: "divider"
                    }}
                  >
                    N
                  </ToggleButton>
                </ToggleButtonGroup>
              </Box>
              
              {/* Participation Toggle */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                  Participation:
                </Typography>
                <ToggleButtonGroup
                  value={entityParticipations[entityName] ?? "partial"}
                  exclusive
                  onChange={(_, newValue) => {
                    if (newValue !== null) {
                      setEntityParticipations({
                        ...entityParticipations,
                        [entityName]: newValue as "total" | "partial"
                      });
                    }
                  }}
                  size="small"
                  sx={{ height: 32 }}
                >
                  <ToggleButton 
                    value="partial" 
                    aria-label="partial"
                    sx={{
                      bgcolor: (entityParticipations[entityName] ?? "partial") === "partial" ? "primary.main" : "white",
                      color: (entityParticipations[entityName] ?? "partial") === "partial" ? "white" : "text.primary",
                      "&.Mui-selected": {
                        bgcolor: "primary.main !important",
                        color: "white !important"
                      },
                      "&:hover": {
                        bgcolor: (entityParticipations[entityName] ?? "partial") === "partial" ? "primary.dark" : "white"
                      },
                      "&.Mui-selected:hover": {
                        bgcolor: "primary.dark !important"
                      },
                      border: "1px solid",
                      borderColor: "divider"
                    }}
                  >
                    Partial
                  </ToggleButton>
                  <ToggleButton 
                    value="total" 
                    aria-label="total"
                    sx={{
                      bgcolor: (entityParticipations[entityName] ?? "partial") === "total" ? "primary.main" : "white",
                      color: (entityParticipations[entityName] ?? "partial") === "total" ? "white" : "text.primary",
                      "&.Mui-selected": {
                        bgcolor: "primary.main !important",
                        color: "white !important"
                      },
                      "&:hover": {
                        bgcolor: (entityParticipations[entityName] ?? "partial") === "total" ? "primary.dark" : "white"
                      },
                      "&.Mui-selected:hover": {
                        bgcolor: "primary.dark !important"
                      },
                      border: "1px solid",
                      borderColor: "divider"
                    }}
                  >
                    Total
                  </ToggleButton>
                </ToggleButtonGroup>
              </Box>
            </Box>
          ))}
        </Box>
      )}
      
      <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
        <Button size="small" onClick={handleSave}>
          Save
        </Button>
        <Button size="small" onClick={onCancel}>
          Cancel
        </Button>
      </Box>
    </Box>
  );
};

// Component for managing constraints in the Add Relation dialog
const NewRelationConstraints: React.FC<{
  entities: string[];
  onConstraintsChange: (cardinalities: Record<string, "1" | "N">, participations: Record<string, "total" | "partial">) => void;
}> = ({ entities, onConstraintsChange }) => {
  const [cardinalities, setCardinalities] = useState<Record<string, "1" | "N">>({});
  const [participations, setParticipations] = useState<Record<string, "total" | "partial">>({});
  
  // Initialize defaults
  useEffect(() => {
    const initCardinalities: Record<string, "1" | "N"> = {};
    const initParticipations: Record<string, "total" | "partial"> = {};
    
    entities.forEach((entityName) => {
      initCardinalities[entityName] = "1";
      initParticipations[entityName] = "partial";
    });
    
    setCardinalities(initCardinalities);
    setParticipations(initParticipations);
    onConstraintsChange(initCardinalities, initParticipations);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entities]);
  
  const handleCardinalityChange = (entityName: string, value: "1" | "N") => {
    const newCardinalities = { ...cardinalities, [entityName]: value };
    setCardinalities(newCardinalities);
    onConstraintsChange(newCardinalities, participations);
  };
  
  const handleParticipationChange = (entityName: string, value: "total" | "partial") => {
    const newParticipations = { ...participations, [entityName]: value };
    setParticipations(newParticipations);
    onConstraintsChange(cardinalities, newParticipations);
  };
  
  return (
    <Box sx={{ mt: 2 }}>
      <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1.5, color: 'text.secondary' }}>
        Entity Constraints:
      </Typography>
      {entities.map((entityName) => (
        <Box 
          key={entityName}
          sx={{ 
            display: 'flex', 
            alignItems: 'center', 
            gap: 2,
            p: 1.5,
            mb: 1.5,
            bgcolor: 'grey.50',
            borderRadius: 1,
            border: '1px solid',
            borderColor: 'divider'
          }}
        >
          <Typography variant="body2" sx={{ minWidth: 100, fontWeight: 500 }}>
            {entityName}:
          </Typography>
          
          {/* Cardinality Toggle */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="caption" sx={{ color: 'text.secondary' }}>
              Cardinality:
            </Typography>
            <ToggleButtonGroup
              value={cardinalities[entityName] || "1"}
              exclusive
              onChange={(_, newValue) => {
                if (newValue !== null) {
                  handleCardinalityChange(entityName, newValue);
                }
              }}
              size="small"
              sx={{ height: 32 }}
            >
              <ToggleButton 
                value="1" 
                aria-label="one"
                sx={{
                  bgcolor: (cardinalities[entityName] || "1") === "1" ? "primary.main" : "white",
                  color: (cardinalities[entityName] || "1") === "1" ? "white" : "text.primary",
                  "&.Mui-selected": {
                    bgcolor: "primary.main !important",
                    color: "white !important"
                  },
                  "&:hover": {
                    bgcolor: (cardinalities[entityName] || "1") === "1" ? "primary.dark" : "white"
                  },
                  "&.Mui-selected:hover": {
                    bgcolor: "primary.dark !important"
                  },
                  border: "1px solid",
                  borderColor: "divider"
                }}
              >
                1
              </ToggleButton>
              <ToggleButton 
                value="N" 
                aria-label="many"
                sx={{
                  bgcolor: (cardinalities[entityName] || "1") === "N" ? "primary.main" : "white",
                  color: (cardinalities[entityName] || "1") === "N" ? "white" : "text.primary",
                  "&.Mui-selected": {
                    bgcolor: "primary.main !important",
                    color: "white !important"
                  },
                  "&:hover": {
                    bgcolor: (cardinalities[entityName] || "1") === "N" ? "primary.dark" : "white"
                  },
                  "&.Mui-selected:hover": {
                    bgcolor: "primary.dark !important"
                  },
                  border: "1px solid",
                  borderColor: "divider"
                }}
              >
                N
              </ToggleButton>
            </ToggleButtonGroup>
          </Box>
          
          {/* Participation Toggle */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="caption" sx={{ color: 'text.secondary' }}>
              Participation:
            </Typography>
            <ToggleButtonGroup
              value={participations[entityName] || "partial"}
              exclusive
              onChange={(_, newValue) => {
                if (newValue !== null) {
                  handleParticipationChange(entityName, newValue);
                }
              }}
              size="small"
              sx={{ height: 32 }}
            >
              <ToggleButton 
                value="partial" 
                aria-label="partial"
                sx={{
                  bgcolor: (participations[entityName] || "partial") === "partial" ? "primary.main" : "white",
                  color: (participations[entityName] || "partial") === "partial" ? "white" : "text.primary",
                  "&.Mui-selected": {
                    bgcolor: "primary.main !important",
                    color: "white !important"
                  },
                  "&:hover": {
                    bgcolor: (participations[entityName] || "partial") === "partial" ? "primary.dark" : "white"
                  },
                  "&.Mui-selected:hover": {
                    bgcolor: "primary.dark !important"
                  },
                  border: "1px solid",
                  borderColor: "divider"
                }}
              >
                Partial
              </ToggleButton>
              <ToggleButton 
                value="total" 
                aria-label="total"
                sx={{
                  bgcolor: (participations[entityName] || "partial") === "total" ? "primary.main" : "white",
                  color: (participations[entityName] || "partial") === "total" ? "white" : "text.primary",
                  "&.Mui-selected": {
                    bgcolor: "primary.main !important",
                    color: "white !important"
                  },
                  "&:hover": {
                    bgcolor: (participations[entityName] || "partial") === "total" ? "primary.dark" : "white"
                  },
                  "&.Mui-selected:hover": {
                    bgcolor: "primary.dark !important"
                  },
                  border: "1px solid",
                  borderColor: "divider"
                }}
              >
                Total
              </ToggleButton>
            </ToggleButtonGroup>
          </Box>
        </Box>
      ))}
    </Box>
  );
};

export default RelationsCheckpoint;
