import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  CircularProgress
} from '@mui/material';
import { PlayArrow } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';
import { API_BASE_URL } from '../../utils/constants';

const ERDiagramCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    proceedToNextCheckpoint,
    markCheckpointCompleted,
    erDiagram
  } = useAppStore();
  
  const { setERDiagram } = useAppStore();
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  useEffect(() => {
    // Update erDiagram state when checkpoint data arrives
    const erDesign = checkpointData?.er_design;
    if (erDesign) {
      setERDiagram({
        entities: erDesign.entities || [],
        relations: erDesign.relations || [],
        imageUrl: erDesign.imageUrl || null
      });
    }
  }, [checkpointData, setERDiagram]);
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Preserve imageUrl from checkpointData when marking as completed
      const erDesignFromData = checkpointData?.er_design || {};
      const erDesignFromStore = erDiagram || {};
      const erDesign = {
        ...erDesignFromData,
        ...erDesignFromStore,
        // Ensure imageUrl is preserved (from checkpointData first, then from store)
        imageUrl: erDesignFromData.imageUrl || erDesignFromStore.imageUrl || null
      };
      
      markCheckpointCompleted("er_diagram", { ...checkpointData, er_design: erDesign });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  // Get image URL and prepend API base URL if it's a relative path
  const rawImageUrl = erDiagram?.imageUrl || checkpointData?.er_design?.imageUrl;
  const imageUrl = rawImageUrl 
    ? (rawImageUrl.startsWith('/') && !rawImageUrl.startsWith('//') 
        ? `${API_BASE_URL}${rawImageUrl}` 
        : rawImageUrl)
    : null;

  // Debug logging
  useEffect(() => {
    if (!imageUrl) {
      console.log('ERDiagramCheckpoint: No imageUrl found', {
        hasErDiagram: !!erDiagram,
        erDiagramImageUrl: erDiagram?.imageUrl,
        hasCheckpointData: !!checkpointData,
        checkpointDataImageUrl: checkpointData?.er_design?.imageUrl,
        fullCheckpointData: checkpointData
      });
    } else {
      console.log('ERDiagramCheckpoint: Image URL found', imageUrl);
    }
  }, [imageUrl, erDiagram, checkpointData]);
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        ER Diagram Review
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
          borderColor: 'divider',
          textAlign: 'center'
        }}
      >
        {imageUrl ? (
          <img
            src={imageUrl}
            alt="ER Diagram"
            style={{ 
              maxWidth: '100%', 
              height: 'auto',
              borderRadius: 8
            }}
          />
        ) : (
          <Typography variant="body2" color="text.secondary">
            ER Diagram image not available
          </Typography>
        )}
      </Paper>
      
      {checkpointJustification && (
        <Box sx={{ mt: 3 }}>
          <JustificationDisplay justification={checkpointJustification} type="er_diagram" />
        </Box>
      )}
      
      <Box sx={{ display: 'flex', gap: 2, justifyContent: 'flex-end' }}>
        <Button
          variant="contained"
          startIcon={saving ? <CircularProgress size={16} /> : <PlayArrow />}
          onClick={handleProceed}
          disabled={saving}
        >
          Proceed
        </Button>
      </Box>
    </Box>
  );
};

export default ERDiagramCheckpoint;
