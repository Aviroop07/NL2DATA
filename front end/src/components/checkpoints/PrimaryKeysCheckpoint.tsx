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
  Chip,
  Autocomplete
} from '@mui/material';
import { Save, PlayArrow } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const PrimaryKeysCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    checkpointData,
    checkpointJustification,
    savePrimaryKeysEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted,
    completedCheckpoints
  } = useAppStore();
  
  const [editedPrimaryKeys, setEditedPrimaryKeys] = useState<Record<string, string[]>>(
    checkpointData?.primary_keys || {}
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    if (checkpointData?.primary_keys) {
      setEditedPrimaryKeys(checkpointData.primary_keys);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    const primaryKeysChanged = JSON.stringify(editedPrimaryKeys) !== JSON.stringify(checkpointData?.primary_keys || {});
    setHasChanges(primaryKeysChanged);
  }, [editedPrimaryKeys, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await savePrimaryKeysEdit(jobId, editedPrimaryKeys);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save primary keys');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save primary keys to backend first to ensure state is updated
      await savePrimaryKeysEdit(jobId, editedPrimaryKeys);
      
      markCheckpointCompleted("primary_keys", { ...checkpointData, primary_keys: editedPrimaryKeys });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const handlePrimaryKeyChange = (entityName: string, primaryKeys: string[]) => {
    setEditedPrimaryKeys({
      ...editedPrimaryKeys,
      [entityName]: primaryKeys
    });
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  // Get attributes from checkpoint data or from previous checkpoints
  const attributesCheckpoint = completedCheckpoints.find(c => c.type === "attributes");
  const entityAttributes = attributesCheckpoint?.data?.attributes || checkpointData?.attributes || {};
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Primary Keys Review
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
          Primary keys for each entity
        </Typography>
        
        <List>
          {entityNames.map((entityName) => {
            const attributes = entityAttributes[entityName] || [];
            const attributeNames = attributes.map((a: any) => a.name || a);
            const currentPK = editedPrimaryKeys[entityName] || [];
            
            return (
              <ListItem
                key={entityName}
                sx={{
                  border: '1px solid',
                  borderColor: 'divider',
                  borderRadius: 1,
                  mb: 1,
                  bgcolor: 'background.paper',
                  flexDirection: 'column',
                  alignItems: 'stretch'
                }}
              >
                <Box sx={{ width: '100%' }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 1 }}>
                    {entityName}
                  </Typography>
                  <Autocomplete
                    multiple
                    options={attributeNames}
                    value={currentPK}
                    onChange={(_, newValue) => handlePrimaryKeyChange(entityName, newValue)}
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        label="Primary Key Attributes"
                        placeholder="Select attributes"
                        size="small"
                      />
                    )}
                    renderTags={(value, getTagProps) =>
                      value.map((option, index) => (
                        <Chip
                          label={option}
                          {...getTagProps({ index })}
                          key={option}
                          color="primary"
                          size="small"
                        />
                      ))
                    }
                  />
                </Box>
              </ListItem>
            );
          })}
        </List>
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="primary_keys" />
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

export default PrimaryKeysCheckpoint;
