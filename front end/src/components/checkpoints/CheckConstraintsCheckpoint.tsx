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
  TextField,
  IconButton
} from '@mui/material';
import { Save, PlayArrow, ExpandMore, Delete, Add } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const CheckConstraintsCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    checkpointData,
    checkpointJustification,
    completedCheckpoints,
    saveCheckConstraintsEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedCheckConstraints, setEditedCheckConstraints] = useState<Record<string, any>>({});
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [lastCheckpointDataKey, setLastCheckpointDataKey] = useState<string>('');
  
  // Initialize editedCheckConstraints when checkpointData changes
  useEffect(() => {
    const checkpointDataKey = JSON.stringify({
      check_constraints: checkpointData?.check_constraints || {}
    });
    
    // Only update if checkpointData actually changed
    if (checkpointDataKey !== lastCheckpointDataKey) {
      setEditedCheckConstraints(checkpointData?.check_constraints || {});
      setLastCheckpointDataKey(checkpointDataKey);
      setHasChanges(false);
    }
  }, [checkpointData, lastCheckpointDataKey]);
  
  useEffect(() => {
    if (!lastCheckpointDataKey) {
      setHasChanges(false);
      return;
    }
    const original = checkpointData?.check_constraints || {};
    const checkConstraintsChanged = JSON.stringify(editedCheckConstraints) !== JSON.stringify(original);
    setHasChanges(checkConstraintsChanged);
  }, [editedCheckConstraints, checkpointData, lastCheckpointDataKey]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveCheckConstraintsEdit(jobId, editedCheckConstraints);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save check constraints');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save check constraints to backend first to ensure state is updated
      await saveCheckConstraintsEdit(jobId, editedCheckConstraints);
      
      markCheckpointCompleted("check_constraints", { 
        ...checkpointData, 
        check_constraints: editedCheckConstraints
      });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const updateCheckConstraint = (entityName: string, attrName: string, field: 'condition' | 'description', value: string) => {
    setEditedCheckConstraints((prev) => {
      const entityConstraints = prev[entityName] || {};
      const attrConstraint = entityConstraints[attrName] || { condition: '', description: '', reasoning: '' };
      
      return {
        ...prev,
        [entityName]: {
          ...entityConstraints,
          [attrName]: {
            ...attrConstraint,
            [field]: value
          }
        }
      };
    });
  };
  
  const removeCheckConstraint = (entityName: string, attrName: string) => {
    setEditedCheckConstraints((prev) => {
      const entityConstraints = { ...(prev[entityName] || {}) };
      delete entityConstraints[attrName];
      
      return {
        ...prev,
        [entityName]: entityConstraints
      };
    });
  };
  
  const addCheckConstraint = (entityName: string, attrName: string) => {
    setEditedCheckConstraints((prev) => {
      const entityConstraints = prev[entityName] || {};
      
      return {
        ...prev,
        [entityName]: {
          ...entityConstraints,
          [attrName]: {
            condition: '',
            description: '',
            reasoning: ''
          }
        }
      };
    });
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Check Constraints Review
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
          Specify check constraints for attributes (e.g., &apos;price &gt; 0&apos;, &apos;age &gt;= 18&apos;)
        </Typography>
        
        {entityNames.map((entityName) => {
          const entityConstraints = editedCheckConstraints[entityName] || {};
          const constraintKeys = Object.keys(entityConstraints);
          
          // Get all attributes for this entity - try checkpointData first, then completed checkpoints
          const attributesCheckpoint = completedCheckpoints.find(c => c.type === "attributes");
          const allAttributes = checkpointData?.attributes?.[entityName] 
            || attributesCheckpoint?.data?.attributes?.[entityName] 
            || [];
          const attributeNames = allAttributes.map((a: any) => a.name || a);
          const attributesWithoutConstraints = attributeNames.filter((attr: string) => !entityConstraints[attr]);
          
          return (
            <Accordion key={entityName} defaultExpanded sx={{ mb: 2 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName} {attributeNames.length > 0 && `(${attributeNames.length} attributes)`}
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                {constraintKeys.length > 0 && (
                  <List>
                    {constraintKeys.map((attrName: string) => {
                      const constraint = entityConstraints[attrName] || { condition: '', description: '', reasoning: '' };
                      
                      return (
                        <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 2, borderBottom: '1px solid', borderColor: 'divider' }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
                            <Typography variant="body2" sx={{ fontWeight: 600 }}>
                              {attrName}
                            </Typography>
                            <IconButton
                              size="small"
                              onClick={() => removeCheckConstraint(entityName, attrName)}
                              color="error"
                            >
                              <Delete fontSize="small" />
                            </IconButton>
                          </Box>
                          <TextField
                            label="Condition"
                            value={constraint.condition || ''}
                            onChange={(e) => updateCheckConstraint(entityName, attrName, 'condition', e.target.value)}
                            placeholder="e.g., price &gt; 0"
                            size="small"
                            fullWidth
                            sx={{ mb: 1 }}
                            helperText="SQL-like condition expression"
                          />
                          <TextField
                            label="Description"
                            value={constraint.description || ''}
                            onChange={(e) => updateCheckConstraint(entityName, attrName, 'description', e.target.value)}
                            placeholder="Human-readable description"
                            size="small"
                            fullWidth
                            multiline
                            rows={2}
                          />
                          {constraint.reasoning && (
                            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5 }}>
                              Reasoning: {constraint.reasoning}
                            </Typography>
                          )}
                        </ListItem>
                      );
                    })}
                  </List>
                )}
                
                {attributesWithoutConstraints.length > 0 && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
                      Add Check Constraint:
                    </Typography>
                    <List>
                      {attributesWithoutConstraints.map((attrName: string) => (
                        <ListItem key={attrName} sx={{ py: 0.5 }}>
                          <Typography variant="body2" sx={{ flex: 1 }}>
                            {attrName}
                          </Typography>
                          <IconButton
                            size="small"
                            onClick={() => addCheckConstraint(entityName, attrName)}
                            color="primary"
                          >
                            <Add fontSize="small" />
                          </IconButton>
                        </ListItem>
                      ))}
                    </List>
                  </Box>
                )}
                
                {constraintKeys.length === 0 && attributesWithoutConstraints.length === 0 && (
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
            <JustificationDisplay justification={checkpointJustification} type="check_constraints" />
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

export default CheckConstraintsCheckpoint;
