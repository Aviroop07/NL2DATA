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
import { Save, PlayArrow, ExpandMore, Delete } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const DefaultValuesCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    checkpointData,
    checkpointJustification,
    completedCheckpoints,
    saveDefaultValuesEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedDefaultValues, setEditedDefaultValues] = useState<Record<string, any>>({});
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [lastCheckpointDataKey, setLastCheckpointDataKey] = useState<string>('');
  
  // Initialize editedDefaultValues when checkpointData changes
  useEffect(() => {
    const checkpointDataKey = JSON.stringify({
      default_values: checkpointData?.default_values || {}
    });
    
    // Only update if checkpointData actually changed
    if (checkpointDataKey !== lastCheckpointDataKey) {
      setEditedDefaultValues(checkpointData?.default_values || {});
      setLastCheckpointDataKey(checkpointDataKey);
      setHasChanges(false);
    }
  }, [checkpointData, lastCheckpointDataKey]);
  
  useEffect(() => {
    if (!lastCheckpointDataKey) {
      setHasChanges(false);
      return;
    }
    const defaultValuesChanged = JSON.stringify(editedDefaultValues) !== JSON.stringify(checkpointData?.default_values || {});
    setHasChanges(defaultValuesChanged);
  }, [editedDefaultValues, checkpointData, lastCheckpointDataKey]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveDefaultValuesEdit(jobId, editedDefaultValues);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save default values');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save default values to backend first to ensure state is updated
      await saveDefaultValuesEdit(jobId, editedDefaultValues);
      
      markCheckpointCompleted("default_values", { 
        ...checkpointData, 
        default_values: editedDefaultValues
      });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const updateDefaultValue = (entityName: string, attrName: string, value: string) => {
    setEditedDefaultValues((prev) => {
      const entityData = prev[entityName] || { default_values: {}, reasoning: {} };
      const defaultValues = { ...entityData.default_values };
      
      if (value.trim()) {
        defaultValues[attrName] = value;
      } else {
        delete defaultValues[attrName];
      }
      
      return {
        ...prev,
        [entityName]: {
          default_values: defaultValues,
          reasoning: entityData.reasoning || {}
        }
      };
    });
  };
  
  const removeDefaultValue = (entityName: string, attrName: string) => {
    setEditedDefaultValues((prev) => {
      const entityData = prev[entityName] || { default_values: {}, reasoning: {} };
      const defaultValues = { ...entityData.default_values };
      delete defaultValues[attrName];
      
      return {
        ...prev,
        [entityName]: {
          default_values: defaultValues,
          reasoning: entityData.reasoning || {}
        }
      };
    });
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Default Values Review
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
          Specify default values for attributes (e.g., '0', 'CURRENT_TIMESTAMP', 'NEW')
        </Typography>
        
        {entityNames.map((entityName) => {
          const entityData = editedDefaultValues[entityName] || { default_values: {}, reasoning: {} };
          const defaultValues = entityData.default_values || {};
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
                      const currentValue = defaultValues[attrName] || '';
                      const attrReasoning = reasoning[attrName] || '';
                      
                      return (
                        <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%', mb: attrReasoning ? 0.5 : 0 }}>
                            <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 120 }}>
                              {attrName}
                            </Typography>
                            <TextField
                              value={currentValue}
                              onChange={(e) => updateDefaultValue(entityName, attrName, e.target.value)}
                              placeholder="Enter default value (e.g., '0', 'CURRENT_TIMESTAMP', 'NEW')"
                              size="small"
                              fullWidth
                              sx={{ flex: 1 }}
                            />
                            {currentValue && (
                              <IconButton
                                size="small"
                                onClick={() => removeDefaultValue(entityName, attrName)}
                                color="error"
                              >
                                <Delete fontSize="small" />
                              </IconButton>
                            )}
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
            <JustificationDisplay justification={checkpointJustification} type="default_values" />
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

export default DefaultValuesCheckpoint;
