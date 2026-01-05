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

const GenerationStrategiesCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    saveGenerationStrategiesEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [generationStrategies, setGenerationStrategies] = useState<Record<string, Record<string, any>>>(
    checkpointData?.generation_strategies || {}
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    if (checkpointData?.generation_strategies) {
      setGenerationStrategies(checkpointData.generation_strategies);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    const strategiesChanged = JSON.stringify(generationStrategies) !== JSON.stringify(checkpointData?.generation_strategies || {});
    setHasChanges(strategiesChanged);
  }, [generationStrategies, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveGenerationStrategiesEdit(jobId, generationStrategies);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save generation strategies');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveGenerationStrategiesEdit(jobId, generationStrategies);
      
      markCheckpointCompleted("generation_strategies", { ...checkpointData, generation_strategies: generationStrategies });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const getStrategyType = (strategy: Record<string, any>): string => {
    if (strategy.distribution) return 'Distribution';
    if (strategy.generator) return 'Generator';
    if (strategy.formula) return 'Formula';
    if (strategy.range) return 'Range';
    return 'Custom';
  };
  
  const formatStrategyDetails = (strategy: Record<string, any>): string => {
    if (strategy.distribution) {
      return `Distribution: ${JSON.stringify(strategy.distribution)}`;
    }
    if (strategy.generator) {
      return `Generator: ${strategy.generator}`;
    }
    if (strategy.formula) {
      return `Formula: ${strategy.formula}`;
    }
    if (strategy.range) {
      return `Range: ${JSON.stringify(strategy.range)}`;
    }
    return JSON.stringify(strategy);
  };
  
  const entityNames = Object.keys(generationStrategies);
  const totalStrategies = Object.values(generationStrategies).reduce(
    (sum, attrs) => sum + Object.keys(attrs).length, 
    0
  );
  
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Generation Strategies Checkpoint
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Review data generation strategies for independent columns. These strategies define how synthetic data will be generated
        for each column based on its type, constraints, and domain requirements.
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
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6">
            Generation Strategies ({totalStrategies} strategies across {entityNames.length} entities)
          </Typography>
        </Box>
        
        {entityNames.length === 0 ? (
          <Alert severity="info">
            No generation strategies found. Strategies will be generated during pipeline execution.
          </Alert>
        ) : (
          entityNames.map((entityName) => {
            const entityStrategies = generationStrategies[entityName] || {};
            const attributeNames = Object.keys(entityStrategies);
            
            if (attributeNames.length === 0) return null;
            
            return (
              <Accordion key={entityName} defaultExpanded>
                <AccordionSummary expandIcon={<ExpandMore />}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
                      {entityName}
                    </Typography>
                    <Chip label={attributeNames.length} size="small" />
                  </Box>
                </AccordionSummary>
                <AccordionDetails>
                  <List>
                    {attributeNames.map((attrName) => {
                      const strategy = entityStrategies[attrName];
                      const strategyType = getStrategyType(strategy);
                      
                      return (
                        <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch' }}>
                          <Box sx={{ width: '100%' }}>
                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                              <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                                {attrName}
                              </Typography>
                              <Chip label={strategyType} size="small" color="primary" />
                            </Box>
                            
                            <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 1 }}>
                              {formatStrategyDetails(strategy)}
                            </Typography>
                            
                            {strategy.reasoning && (
                              <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontStyle: 'italic' }}>
                                Reasoning: {strategy.reasoning}
                              </Typography>
                            )}
                          </Box>
                        </ListItem>
                      );
                    })}
                  </List>
                </AccordionDetails>
              </Accordion>
            );
          })
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

export default GenerationStrategiesCheckpoint;
