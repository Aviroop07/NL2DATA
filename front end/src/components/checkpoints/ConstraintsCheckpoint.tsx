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
  Chip,
  List,
  ListItem,
  ListItemText
} from '@mui/material';
import { Save, PlayArrow, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const ConstraintsCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    saveConstraintsEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [constraints, setConstraints] = useState<Array<Record<string, any>>>(
    checkpointData?.constraints || []
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    if (checkpointData?.constraints) {
      setConstraints(checkpointData.constraints);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    const constraintsChanged = JSON.stringify(constraints) !== JSON.stringify(checkpointData?.constraints || []);
    setHasChanges(constraintsChanged);
  }, [constraints, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveConstraintsEdit(jobId, constraints);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save constraints');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveConstraintsEdit(jobId, constraints);
      
      markCheckpointCompleted("constraints", { ...checkpointData, constraints });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  // Group constraints by category
  const constraintsByCategory = constraints.reduce((acc, constraint) => {
    const category = constraint.constraint_category || constraint.category || 'other_constraints';
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(constraint);
    return acc;
  }, {} as Record<string, Array<Record<string, any>>>);
  
  const categoryLabels: Record<string, string> = {
    statistical_constraints: 'Statistical Constraints',
    structural_constraints: 'Structural Constraints',
    distribution_constraints: 'Distribution Constraints',
    other_constraints: 'Other Constraints'
  };
  
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Constraints Checkpoint
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Review constraints identified for the schema. Constraints are grouped by category.
        These constraints will be used to guide data generation strategies.
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}
      
      {checkpointJustification && (
        <Box sx={{ mb: 3 }}>
          <JustificationDisplay justification={checkpointJustification} />
        </Box>
      )}
      
      <Paper sx={{ p: 2, mb: 2 }}>
        <Typography variant="h6" sx={{ mb: 2 }}>
          Constraints ({constraints.length} total)
        </Typography>
        
        {constraints.length === 0 ? (
          <Alert severity="info">
            No constraints found. Constraints will be identified during pipeline execution.
          </Alert>
        ) : (
          Object.entries(constraintsByCategory).map(([category, categoryConstraints]) => (
            <Accordion key={category} defaultExpanded>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
                    {categoryLabels[category] || category}
                  </Typography>
                  <Chip label={categoryConstraints.length} size="small" />
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <List>
                  {categoryConstraints.map((constraint, index) => (
                    <ListItem key={index} sx={{ flexDirection: 'column', alignItems: 'stretch' }}>
                      <Box sx={{ width: '100%', mb: 1 }}>
                        <Typography variant="body2" sx={{ fontWeight: 'bold', mb: 1 }}>
                          {constraint.description || 'No description'}
                        </Typography>
                        
                        {constraint.affected_entities && constraint.affected_entities.length > 0 && (
                          <Box sx={{ mb: 1 }}>
                            <Typography variant="caption" color="text.secondary">
                              Entities: {constraint.affected_entities.join(', ')}
                            </Typography>
                          </Box>
                        )}
                        
                        {constraint.affected_attributes && constraint.affected_attributes.length > 0 && (
                          <Box sx={{ mb: 1 }}>
                            <Typography variant="caption" color="text.secondary">
                              Attributes: {constraint.affected_attributes.join(', ')}
                            </Typography>
                          </Box>
                        )}
                        
                        {constraint.enforcement_strategy && (
                          <Box sx={{ mt: 1 }}>
                            <Chip 
                              label={`Enforcement: ${constraint.enforcement_strategy}`} 
                              size="small" 
                              color="primary"
                            />
                          </Box>
                        )}
                        
                        {constraint.reasoning && (
                          <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                            Reasoning: {constraint.reasoning}
                          </Typography>
                        )}
                      </Box>
                    </ListItem>
                  ))}
                </List>
              </AccordionDetails>
            </Accordion>
          ))
        )}
      </Paper>
      
      <Box sx={{ display: 'flex', gap: 2, justifyContent: 'flex-end', mt: 3 }}>
        <Button
          variant="outlined"
          onClick={handleSave}
          disabled={saving || !hasChanges}
          startIcon={saving ? <CircularProgress size={16} /> : <Save />}
        >
          Save
        </Button>
        <Button
          variant="contained"
          onClick={handleProceed}
          disabled={saving}
          startIcon={saving ? <CircularProgress size={16} /> : <PlayArrow />}
        >
          Save & Proceed
        </Button>
      </Box>
    </Box>
  );
};

export default ConstraintsCheckpoint;
