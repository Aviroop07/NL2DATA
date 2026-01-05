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
  TextField
} from '@mui/material';
import { Save, PlayArrow, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const DatatypesCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    saveDatatypesEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedDataTypes, setEditedDataTypes] = useState<Record<string, any>>(
    checkpointData?.data_types || {}
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    if (checkpointData?.data_types) {
      setEditedDataTypes(checkpointData.data_types);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    // Check if datatypes have changed
    const dataTypesChanged = JSON.stringify(editedDataTypes) !== JSON.stringify(checkpointData?.data_types || {});
    setHasChanges(dataTypesChanged);
  }, [editedDataTypes, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveDatatypesEdit(jobId, editedDataTypes);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save datatypes');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save datatypes to backend first to ensure state is updated
      await saveDatatypesEdit(jobId, editedDataTypes);
      
      markCheckpointCompleted("datatypes", { ...checkpointData, data_types: editedDataTypes });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const handleTypeChange = (entityName: string, attributeName: string, field: string, value: any) => {
    setEditedDataTypes(prev => {
      const updated = { ...prev };
      if (!updated[entityName]) {
        updated[entityName] = { attribute_types: {} };
      }
      if (!updated[entityName].attribute_types) {
        updated[entityName].attribute_types = {};
      }
      if (!updated[entityName].attribute_types[attributeName]) {
        updated[entityName].attribute_types[attributeName] = {};
      }
      updated[entityName].attribute_types[attributeName][field] = value;
      return updated;
    });
  };
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Data Type Assignment Review
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
          {Object.keys(editedDataTypes).length} entities with assigned data types
        </Typography>
        
        {Object.entries(editedDataTypes).map(([entityName, entityData]) => {
          const attributeTypes = entityData?.attribute_types || entityData || {};
          
          return (
            <Accordion key={entityName} sx={{ mb: 1 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName}
                </Typography>
                <Chip 
                  label={`${Object.keys(attributeTypes).length} attributes`} 
                  size="small" 
                  sx={{ ml: 2 }}
                />
              </AccordionSummary>
              <AccordionDetails>
                <List dense>
                  {Object.entries(attributeTypes).map(([attrName, typeInfo]: [string, any]) => {
                    const type = typeInfo?.type || '';
                    const size = typeInfo?.size;
                    const precision = typeInfo?.precision;
                    const scale = typeInfo?.scale;
                    const reasoning = typeInfo?.reasoning || '';
                    
                    // Build type string
                    let typeString = type;
                    if (type && (type.toUpperCase() === "VARCHAR" || type.toUpperCase() === "CHAR")) {
                      typeString = size ? `${type}(${size})` : `${type}(255)`;
                    } else if (type && (type.toUpperCase() === "DECIMAL" || type.toUpperCase() === "NUMERIC")) {
                      if (precision !== null && precision !== undefined && scale !== null && scale !== undefined) {
                        typeString = `${type}(${precision},${scale})`;
                      } else if (precision !== null && precision !== undefined) {
                        typeString = `${type}(${precision})`;
                      }
                    }
                    
                    return (
                      <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1.5 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
                          <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 150 }}>
                            {attrName}:
                          </Typography>
                          <Chip 
                            label={typeString || 'Not assigned'} 
                            size="small" 
                            color="primary"
                            variant="outlined"
                          />
                        </Box>
                        {reasoning && (
                          <Typography variant="caption" color="text.secondary" sx={{ pl: 2 }}>
                            {reasoning}
                          </Typography>
                        )}
                      </ListItem>
                    );
                  })}
                </List>
              </AccordionDetails>
            </Accordion>
          );
        })}
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="datatypes" />
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

export default DatatypesCheckpoint;
