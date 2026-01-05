import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  CircularProgress
} from '@mui/material';
import { Save, PlayArrow } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';
import RelationalSchemaEditor from '../schema/RelationalSchemaEditor';

const RelationalSchemaCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    saveRelationalSchemaEdit,
    markCheckpointCompleted,
    relationalSchema
  } = useAppStore();
  
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    // Check if relational schema has changed
    const schema = checkpointData?.relational_schema;
    if (schema && relationalSchema) {
      const schemaChanged = JSON.stringify(schema) !== JSON.stringify(relationalSchema);
      setHasChanges(schemaChanged);
    }
  }, [checkpointData, relationalSchema]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      const schema = relationalSchema || checkpointData?.relational_schema || {};
      await saveRelationalSchemaEdit(jobId, schema);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save relational schema');
      setSaving(false);
    }
  };
  
  const handleComplete = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      const schema = relationalSchema || checkpointData?.relational_schema || {};
      
      // Save relational schema to backend first to ensure state is updated
      await saveRelationalSchemaEdit(jobId, schema);
      
      markCheckpointCompleted("relational_schema", { ...checkpointData, relational_schema: schema });
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to complete');
      setSaving(false);
    }
  };
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Relational Schema Review
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
        <RelationalSchemaEditor />
      </Paper>
      
      {checkpointJustification && (
        <Box sx={{ mt: 3 }}>
          <JustificationDisplay justification={checkpointJustification} type="relational_schema" />
        </Box>
      )}
      
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
          onClick={handleComplete}
          disabled={saving || hasChanges}
        >
          Complete
        </Button>
      </Box>
    </Box>
  );
};

export default RelationalSchemaCheckpoint;
