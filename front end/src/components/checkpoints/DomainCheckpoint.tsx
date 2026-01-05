import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Alert,
  CircularProgress
} from '@mui/material';
import { Save, PlayArrow } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const DomainCheckpoint: React.FC = () => {
  const { 
    jobId, 
    domain, 
    checkpointData, 
    checkpointJustification,
    saveDomainEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedDomain, setEditedDomain] = useState(domain || '');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    setHasChanges(editedDomain.trim() !== (domain || '').trim());
  }, [editedDomain, domain]);
  
  const handleSave = async () => {
    if (!jobId || !editedDomain.trim()) {
      setError('Domain cannot be empty');
      return;
    }
    
    setSaving(true);
    setError(null);
    
    try {
      await saveDomainEdit(jobId!, editedDomain.trim());
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save domain');
    } finally {
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save domain to backend first to ensure state is updated
      await saveDomainEdit(jobId, editedDomain);
      
      // Mark current checkpoint as completed before proceeding (with current data)
      markCheckpointCompleted("domain", { ...checkpointData, domain: editedDomain });
      // proceedToNextCheckpoint will clear currentCheckpoint and show read-only version
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Domain Review
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
        <Typography variant="subtitle2" sx={{ mb: 1, color: 'text.secondary' }}>
          Detected Domain
        </Typography>
        <TextField
          fullWidth
          value={editedDomain}
          onChange={(e) => setEditedDomain(e.target.value)}
          placeholder="Enter domain name"
          variant="outlined"
          sx={{ mb: 2 }}
        />
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="domain" />
          </Box>
        )}
      </Paper>
      
      <Box sx={{ display: 'flex', gap: 2, justifyContent: 'flex-end' }}>
        <Button
          variant="outlined"
          startIcon={saving ? <CircularProgress size={16} /> : <Save />}
          onClick={handleSave}
          disabled={saving || !hasChanges || !editedDomain.trim()}
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

export default DomainCheckpoint;
