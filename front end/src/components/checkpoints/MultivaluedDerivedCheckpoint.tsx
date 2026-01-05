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
  FormControlLabel,
  Switch,
  Chip
} from '@mui/material';
import { Save, PlayArrow, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const MultivaluedDerivedCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    checkpointData,
    checkpointJustification,
    completedCheckpoints,
    saveMultivaluedDerivedEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedMultivaluedDerived, setEditedMultivaluedDerived] = useState<Record<string, any>>({});
  const [editedDerivedFormulas, setEditedDerivedFormulas] = useState<Record<string, any>>({});
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [lastCheckpointDataKey, setLastCheckpointDataKey] = useState<string>('');
  
  // Initialize editedMultivaluedDerived and editedDerivedFormulas when checkpointData changes
  useEffect(() => {
    const checkpointDataKey = JSON.stringify({
      multivalued_derived: checkpointData?.multivalued_derived || {},
      derived_formulas: checkpointData?.derived_formulas || {}
    });
    
    // Only update if checkpointData actually changed
    if (checkpointDataKey !== lastCheckpointDataKey) {
      setEditedMultivaluedDerived(checkpointData?.multivalued_derived || {});
      setEditedDerivedFormulas(checkpointData?.derived_formulas || {});
      setLastCheckpointDataKey(checkpointDataKey);
      setHasChanges(false);
    }
  }, [checkpointData, lastCheckpointDataKey]);
  
  useEffect(() => {
    if (!lastCheckpointDataKey) {
      setHasChanges(false);
      return;
    }
    const multivaluedChanged = JSON.stringify(editedMultivaluedDerived) !== JSON.stringify(checkpointData?.multivalued_derived || {});
    const formulasChanged = JSON.stringify(editedDerivedFormulas) !== JSON.stringify(checkpointData?.derived_formulas || {});
    setHasChanges(multivaluedChanged || formulasChanged);
  }, [editedMultivaluedDerived, editedDerivedFormulas, checkpointData, lastCheckpointDataKey]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveMultivaluedDerivedEdit(jobId, editedMultivaluedDerived, editedDerivedFormulas);
      // After saving, the backend has been updated
      // The hasChanges will be recalculated on next render when checkpointData updates
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save multivalued/derived attributes');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save multivalued/derived to backend first to ensure state is updated
      await saveMultivaluedDerivedEdit(jobId, editedMultivaluedDerived, editedDerivedFormulas);
      
      markCheckpointCompleted("multivalued_derived", { 
        ...checkpointData, 
        multivalued_derived: editedMultivaluedDerived,
        derived_formulas: editedDerivedFormulas
      });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const toggleMultivalued = (entityName: string, attrName: string, isMultivalued: boolean) => {
    setEditedMultivaluedDerived((prev) => {
      const entityData = prev[entityName] || { multivalued: [], derived: [], derivation_rules: {}, multivalued_handling: {} };
      const multivalued = [...(entityData.multivalued || [])];
      
      if (isMultivalued) {
        if (!multivalued.includes(attrName)) {
          multivalued.push(attrName);
        }
      } else {
        const index = multivalued.indexOf(attrName);
        if (index > -1) {
          multivalued.splice(index, 1);
        }
      }
      
      return {
        ...prev,
        [entityName]: {
          ...entityData,
          multivalued
        }
      };
    });
  };
  
  const toggleDerived = (entityName: string, attrName: string, isDerived: boolean) => {
    setEditedMultivaluedDerived((prev) => {
      const entityData = prev[entityName] || { multivalued: [], derived: [], derivation_rules: {}, multivalued_handling: {} };
      const derived = [...(entityData.derived || [])];
      
      if (isDerived) {
        if (!derived.includes(attrName)) {
          derived.push(attrName);
        }
      } else {
        const index = derived.indexOf(attrName);
        if (index > -1) {
          derived.splice(index, 1);
        }
        // Remove formula when unmarking as derived
        const formulaKey = `${entityName}.${attrName}`;
        setEditedDerivedFormulas((formulas) => {
          const newFormulas = { ...formulas };
          delete newFormulas[formulaKey];
          return newFormulas;
        });
      }
      
      return {
        ...prev,
        [entityName]: {
          ...entityData,
          derived
        }
      };
    });
  };
  
  const updateDerivedFormula = (entityName: string, attrName: string, formula: string) => {
    const formulaKey = `${entityName}.${attrName}`;
    setEditedDerivedFormulas((prev) => {
      const formulaInfo = prev[formulaKey] || {};
      return {
        ...prev,
        [formulaKey]: {
          ...formulaInfo,
          formula
        }
      };
    });
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Multivalued and Derived Attributes Review
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
          Mark attributes as multivalued or derived. For derived attributes, specify the DSL formula.
        </Typography>
        
        {entityNames.map((entityName) => {
          const entityData = editedMultivaluedDerived[entityName] || {};
          const multivalued = entityData.multivalued || [];
          const derived = entityData.derived || [];
          
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
                      const isMultivalued = multivalued.includes(attrName);
                      const isDerived = derived.includes(attrName);
                      const formulaKey = `${entityName}.${attrName}`;
                      const formulaInfo = editedDerivedFormulas[formulaKey] || {};
                      const formula = formulaInfo.formula || '';
                      
                      return (
                        <ListItem 
                          key={attrName} 
                          sx={{ 
                            flexDirection: 'column', 
                            alignItems: 'stretch', 
                            py: 2,
                            borderBottom: '1px solid',
                            borderColor: 'divider'
                          }}
                        >
                          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
                            <Typography variant="body2" sx={{ fontWeight: 600 }}>
                              {attrName}
                            </Typography>
                            <Box sx={{ display: 'flex', gap: 2 }}>
                              <FormControlLabel
                                control={
                                  <Switch
                                    checked={isMultivalued}
                                    onChange={(e) => toggleMultivalued(entityName, attrName, e.target.checked)}
                                    size="small"
                                  />
                                }
                                label="Multivalued"
                                sx={{ m: 0 }}
                              />
                              <FormControlLabel
                                control={
                                  <Switch
                                    checked={isDerived}
                                    onChange={(e) => toggleDerived(entityName, attrName, e.target.checked)}
                                    size="small"
                                  />
                                }
                                label="Derived"
                                sx={{ m: 0 }}
                              />
                            </Box>
                          </Box>
                          
                          {isDerived && (
                            <Box sx={{ mt: 1 }}>
                              <TextField
                                label="DSL Formula"
                                value={formula}
                                onChange={(e) => updateDerivedFormula(entityName, attrName, e.target.value)}
                                placeholder="e.g., quantity * unit_price"
                                size="small"
                                fullWidth
                                required
                                helperText="Enter the derivation formula in DSL syntax"
                              />
                              {formulaInfo.dependencies && formulaInfo.dependencies.length > 0 && (
                                <Box sx={{ mt: 0.5 }}>
                                  <Typography variant="caption" color="text.secondary">
                                    Dependencies: {formulaInfo.dependencies.join(', ')}
                                  </Typography>
                                </Box>
                              )}
                            </Box>
                          )}
                          
                          {(isMultivalued || isDerived) && (
                            <Box sx={{ mt: 1, display: 'flex', gap: 0.5 }}>
                              {isMultivalued && (
                                <Chip label="Multivalued" size="small" color="primary" />
                              )}
                              {isDerived && (
                                <Chip label="Derived" size="small" color="secondary" />
                              )}
                            </Box>
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
            <JustificationDisplay justification={checkpointJustification} type="multivalued_derived" />
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

export default MultivaluedDerivedCheckpoint;
