import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  CircularProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  Chip,
  ToggleButtonGroup,
  ToggleButton
} from '@mui/material';
import type { Relation } from '../../types/state';
import { Save, PlayArrow, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const NullabilityCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    relations,
    checkpointData,
    checkpointJustification,
    completedCheckpoints,
    saveNullabilityEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedNullability, setEditedNullability] = useState<Record<string, any>>({});
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [lastCheckpointDataKey, setLastCheckpointDataKey] = useState<string>('');
  
  // Helper function to check if an attribute is a foreign key in a total participation relation
  const isFKInTotalParticipation = (entityName: string, attrName: string): boolean => {
    if (!relations) return false;
    
    // Check all relations involving this entity
    for (const relation of relations) {
      if (!relation.entities.includes(entityName)) continue;
      
      // Check if this relation has total participation for this entity
      const participation = relation.entity_participations?.[entityName];
      if (participation === 'total') {
        // Check if the attribute name matches a related entity (suggesting it's a FK)
        const otherEntities = relation.entities.filter(e => e !== entityName);
        for (const otherEntity of otherEntities) {
          // Common FK naming patterns: entity_id, id_entity, or just entity name
          if (
            attrName === `${otherEntity}_id` ||
            attrName === `id_${otherEntity}` ||
            attrName === otherEntity ||
            attrName.toLowerCase() === otherEntity.toLowerCase()
          ) {
            return true;
          }
        }
      }
    }
    
    return false;
  };
  
  // Initialize editedNullability when checkpointData changes
  useEffect(() => {
    if (checkpointData?.nullability) {
      const checkpointDataKey = JSON.stringify({
        nullability: checkpointData.nullability,
        primary_keys: checkpointData.primary_keys,
        attributes: checkpointData.attributes
      });
      
      // Only update if checkpointData actually changed
      if (checkpointDataKey !== lastCheckpointDataKey) {
        let updatedNullability = { ...checkpointData.nullability };
        
        // Ensure PKs and FKs in total participation are NOT NULL
        const entityNames = entities?.map(e => e.name) || [];
        const primaryKeys = checkpointData?.primary_keys || {};
        
        entityNames.forEach((entityName) => {
          const entityData = updatedNullability[entityName] || { nullable: [], non_nullable: [] };
          const nullable = [...(entityData.nullable || [])];
          const nonNullable = [...(entityData.non_nullable || [])];
          
          // Get all attributes for this entity
          const allAttributes = checkpointData?.attributes?.[entityName] || [];
          const attributeNames = allAttributes.map((a: any) => a.name || a);
          
          attributeNames.forEach((attrName: string) => {
            const isPK = (primaryKeys[entityName] || []).includes(attrName);
            const isFKTotal = isFKInTotalParticipation(entityName, attrName);
            
            if (isPK || isFKTotal) {
              // Remove from nullable, ensure in non_nullable
              const nullableIndex = nullable.indexOf(attrName);
              if (nullableIndex > -1) {
                nullable.splice(nullableIndex, 1);
              }
              if (!nonNullable.includes(attrName)) {
                nonNullable.push(attrName);
              }
            }
          });
          
          updatedNullability[entityName] = {
            nullable,
            non_nullable: nonNullable,
            reasoning: entityData.reasoning || {}
          };
        });
        
        setEditedNullability(updatedNullability);
        setLastCheckpointDataKey(checkpointDataKey);
        setHasChanges(false);
      }
    }
  }, [checkpointData, entities, relations, lastCheckpointDataKey]);
  
  useEffect(() => {
    const nullabilityChanged = JSON.stringify(editedNullability) !== JSON.stringify(checkpointData?.nullability || {});
    setHasChanges(nullabilityChanged);
  }, [editedNullability, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveNullabilityEdit(jobId, editedNullability);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save nullability constraints');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save nullability to backend first to ensure state is updated
      await saveNullabilityEdit(jobId, editedNullability);
      
      markCheckpointCompleted("nullability", { 
        ...checkpointData, 
        nullability: editedNullability
      });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const toggleAttributeNullability = (entityName: string, attrName: string, isNullable: boolean) => {
    setEditedNullability((prev) => {
      const entityData = prev[entityName] || { nullable: [], non_nullable: [] };
      const nullable = [...(entityData.nullable || [])];
      const nonNullable = [...(entityData.non_nullable || [])];
      
      if (isNullable) {
        // Move from non_nullable to nullable
        const index = nonNullable.indexOf(attrName);
        if (index > -1) {
          nonNullable.splice(index, 1);
        }
        if (!nullable.includes(attrName)) {
          nullable.push(attrName);
        }
      } else {
        // Move from nullable to non_nullable
        const index = nullable.indexOf(attrName);
        if (index > -1) {
          nullable.splice(index, 1);
        }
        if (!nonNullable.includes(attrName)) {
          nonNullable.push(attrName);
        }
      }
      
      return {
        ...prev,
        [entityName]: {
          nullable,
          non_nullable: nonNullable,
          reasoning: entityData.reasoning || {}
        }
      };
    });
  };
  
  // Helper function to check if an attribute is a primary key
  const isPrimaryKey = (entityName: string, attrName: string): boolean => {
    const primaryKeys = checkpointData?.primary_keys || {};
    const entityPKs = primaryKeys[entityName] || [];
    return entityPKs.includes(attrName);
  };
  
  
  const entityNames = entities?.map(e => e.name) || [];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Nullability Constraints Review
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
        <Typography variant="subtitle2" sx={{ mb: 2, color: 'text.secondary' }}>
          Specify which attributes can be NULL. Primary keys are automatically NOT NULL.
        </Typography>
        
        {entityNames.map((entityName) => {
          const entityData = editedNullability[entityName] || { nullable: [], non_nullable: [] };
          const nullable = entityData.nullable || [];
          const nonNullable = entityData.non_nullable || [];
          const reasoning = entityData.reasoning || {};
          
          // Get all attributes for this entity - try checkpointData first, then completed checkpoints
          const attributesCheckpoint = completedCheckpoints.find(c => c.type === "attributes");
          const allAttributes = checkpointData?.attributes?.[entityName] 
            || attributesCheckpoint?.data?.attributes?.[entityName] 
            || [];
          const attributeNames = allAttributes.map((a: any) => a.name || a);
          
          return (
            <Accordion key={entityName} defaultExpanded sx={{ mb: 2 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName} {attributeNames.length > 0 && `(${attributeNames.length} attributes)`}
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                {attributeNames.length > 0 ? (
                  <List>
                    {attributeNames.map((attrName: string) => {
                      const isPK = isPrimaryKey(entityName, attrName);
                      const isFKTotal = isFKInTotalParticipation(entityName, attrName);
                      const isRequired = isPK || isFKTotal;
                      const isNullable = !isRequired && nullable.includes(attrName);
                      const attrReasoning = reasoning[attrName] || '';
                      
                      return (
                        <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', mb: attrReasoning ? 0.5 : 0 }}>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 120 }}>
                                {attrName}
                              </Typography>
                              {isPK && (
                                <Chip label="PK" size="small" color="primary" sx={{ height: 20 }} />
                              )}
                              {isFKTotal && (
                                <Chip label="FK (Total)" size="small" color="secondary" sx={{ height: 20 }} />
                              )}
                            </Box>
                            <ToggleButtonGroup
                              value={isNullable ? "nullable" : "not_null"}
                              exclusive
                              onChange={(_, newValue) => {
                                if (newValue !== null && !isRequired) {
                                  toggleAttributeNullability(entityName, attrName, newValue === "nullable");
                                }
                              }}
                              size="small"
                              disabled={isRequired}
                              sx={{ height: 32 }}
                            >
                              <ToggleButton 
                                value="nullable" 
                                aria-label="nullable"
                                disabled={isRequired}
                                sx={{
                                  bgcolor: isNullable && !isRequired ? "primary.main" : "white",
                                  color: isNullable && !isRequired ? "white" : "text.primary",
                                  "&.Mui-selected": {
                                    bgcolor: "primary.main !important",
                                    color: "white !important"
                                  },
                                  "&:hover": {
                                    bgcolor: isNullable && !isRequired ? "primary.dark" : "white"
                                  },
                                  "&.Mui-selected:hover": {
                                    bgcolor: "primary.dark !important"
                                  },
                                  "&.Mui-disabled": {
                                    bgcolor: "grey.100",
                                    color: "text.disabled"
                                  },
                                  border: "1px solid",
                                  borderColor: "divider"
                                }}
                              >
                                Nullable
                              </ToggleButton>
                              <ToggleButton 
                                value="not_null" 
                                aria-label="not null"
                                disabled={isRequired}
                                sx={{
                                  bgcolor: !isNullable && !isRequired ? "primary.main" : "white",
                                  color: !isNullable && !isRequired ? "white" : "text.primary",
                                  "&.Mui-selected": {
                                    bgcolor: "primary.main !important",
                                    color: "white !important"
                                  },
                                  "&:hover": {
                                    bgcolor: !isNullable && !isRequired ? "primary.dark" : "white"
                                  },
                                  "&.Mui-selected:hover": {
                                    bgcolor: "primary.dark !important"
                                  },
                                  "&.Mui-disabled": {
                                    bgcolor: "grey.100",
                                    color: "text.disabled"
                                  },
                                  border: "1px solid",
                                  borderColor: "divider"
                                }}
                              >
                                NOT NULL
                              </ToggleButton>
                            </ToggleButtonGroup>
                          </Box>
                          {attrReasoning && (
                            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                              {attrReasoning}
                            </Typography>
                          )}
                        </ListItem>
                      );
                    })}
                  </List>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No attributes found for this entity.
                  </Typography>
                )}
              </AccordionDetails>
            </Accordion>
          );
        })}
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="nullability" />
          </Box>
        )}
      </Paper>
      
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

export default NullabilityCheckpoint;
