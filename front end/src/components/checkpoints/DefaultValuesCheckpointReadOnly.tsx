import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List, ListItem, Chip } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

interface DefaultValuesCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const DefaultValuesCheckpointReadOnly: React.FC<DefaultValuesCheckpointReadOnlyProps> = ({ data, justification }) => {
  const { completedCheckpoints } = useAppStore();
  const defaultValues = data.default_values || {};
  
  // Get attributes from data first, then from completed checkpoints
  const attributesCheckpoint = completedCheckpoints.find(c => c.type === "attributes");
  const attributes = data.attributes || attributesCheckpoint?.data?.attributes || {};
  
  // Get all entity names from attributes (to show all entities, not just those with default values)
  const entityNamesFromAttributes = Object.keys(attributes);
  const entityNamesFromDefaults = Object.keys(defaultValues);
  // Combine and deduplicate
  const allEntityNames = Array.from(new Set([...entityNamesFromAttributes, ...entityNamesFromDefaults]));
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Default Values Review (Completed)
        </Typography>
      </Box>
      
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
        {allEntityNames.map((entityName) => {
          const entityData = defaultValues[entityName] || { default_values: {}, reasoning: {} };
          const defaults = entityData.default_values || {};
          const reasoning = entityData.reasoning || {};
          
          const allAttributes = attributes[entityName] || [];
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
                      const defaultValue = defaults[attrName];
                      const attrReasoning = reasoning[attrName] || '';
                      
                      return (
                        <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: attrReasoning ? 0.5 : 0 }}>
                            <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 120 }}>
                              {attrName}
                            </Typography>
                            {defaultValue ? (
                              <Typography variant="body2" sx={{ fontFamily: 'monospace', bgcolor: 'grey.100', px: 1, py: 0.5, borderRadius: 1 }}>
                                {defaultValue}
                              </Typography>
                            ) : (
                              <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                No default value
                              </Typography>
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
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="default_values" />}
    </Box>
  );
};

export default DefaultValuesCheckpointReadOnly;
