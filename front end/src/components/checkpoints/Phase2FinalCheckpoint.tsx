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
  Chip
} from '@mui/material';
import { Save, PlayArrow, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const Phase2FinalCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    checkpointData,
    checkpointJustification,
    savePhase2FinalEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedAttributes, setEditedAttributes] = useState<Record<string, any[]>>(
    checkpointData?.attributes || {}
  );
  const [editedRelationAttributes, setEditedRelationAttributes] = useState<Record<string, any>>(
    checkpointData?.relation_attributes || {}
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    if (checkpointData?.attributes) {
      setEditedAttributes(checkpointData.attributes);
    }
    if (checkpointData?.relation_attributes) {
      setEditedRelationAttributes(checkpointData.relation_attributes);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    const attributesChanged = JSON.stringify(editedAttributes) !== JSON.stringify(checkpointData?.attributes || {});
    const relationAttributesChanged = JSON.stringify(editedRelationAttributes) !== JSON.stringify(checkpointData?.relation_attributes || {});
    setHasChanges(attributesChanged || relationAttributesChanged);
  }, [editedAttributes, editedRelationAttributes, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await savePhase2FinalEdit(jobId, editedAttributes, editedRelationAttributes);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save phase 2 final data');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save phase2 final data to backend first to ensure state is updated
      await savePhase2FinalEdit(jobId, editedAttributes, editedRelationAttributes);
      
      markCheckpointCompleted("phase2_final", { 
        ...checkpointData, 
        attributes: editedAttributes,
        relation_attributes: editedRelationAttributes
      });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Phase 2 Final Review
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
          Review final entity attributes and relation attributes after Phase 2 cleanup and reconciliation
        </Typography>
        
        <Accordion defaultExpanded sx={{ mb: 2 }}>
          <AccordionSummary expandIcon={<ExpandMore />}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Entity Attributes
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            {entityNames.map((entityName) => {
              const attrs = editedAttributes[entityName] || [];
              
              return (
                <Accordion key={entityName} sx={{ mb: 1 }}>
                  <AccordionSummary expandIcon={<ExpandMore />}>
                    <Typography variant="body2" sx={{ fontWeight: 500 }}>
                      {entityName} ({attrs.length} attributes)
                    </Typography>
                  </AccordionSummary>
                  <AccordionDetails>
                    {attrs.length > 0 ? (
                      <List dense>
                        {attrs.map((attr: any, index: number) => {
                          const attrName = attr.name || attr;
                          const attrDesc = attr.description || '';
                          
                          return (
                            <ListItem key={index} sx={{ py: 0.5 }}>
                              <Chip label={attrName} size="small" sx={{ mr: 1 }} />
                              {attrDesc && (
                                <Typography variant="caption" color="text.secondary">
                                  {attrDesc}
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
          </AccordionDetails>
        </Accordion>
        
        <Accordion sx={{ mb: 2 }}>
          <AccordionSummary expandIcon={<ExpandMore />}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Relation Attributes
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            {Object.keys(editedRelationAttributes).length > 0 ? (
              <List>
                {Object.entries(editedRelationAttributes).map(([relationId, relationData]: [string, any]) => {
                  const relationAttrs = relationData.relation_attributes || [];
                  const hasAttributes = relationData.has_attributes || false;
                  
                  return (
                    <ListItem key={relationId} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                        {relationId}
                      </Typography>
                      {hasAttributes && relationAttrs.length > 0 ? (
                        <List dense>
                          {relationAttrs.map((attr: any, index: number) => {
                            const attrName = attr.name || attr;
                            return (
                              <ListItem key={index} sx={{ py: 0.25, pl: 2 }}>
                                <Chip label={attrName} size="small" />
                              </ListItem>
                            );
                          })}
                        </List>
                      ) : (
                        <Typography variant="caption" color="text.secondary">
                          No relation attributes
                        </Typography>
                      )}
                    </ListItem>
                  );
                })}
              </List>
            ) : (
              <Typography variant="body2" color="text.secondary">
                No relation attributes found.
              </Typography>
            )}
          </AccordionDetails>
        </Accordion>
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="phase2_final" />
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

export default Phase2FinalCheckpoint;
