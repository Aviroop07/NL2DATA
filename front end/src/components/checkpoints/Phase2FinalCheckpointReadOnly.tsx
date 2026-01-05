import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List, ListItem, Chip } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface Phase2FinalCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const Phase2FinalCheckpointReadOnly: React.FC<Phase2FinalCheckpointReadOnlyProps> = ({ data, justification }) => {
  const attributes = data.attributes || {};
  const relationAttributes = data.relation_attributes || {};
  
  const entityNames = Object.keys(attributes);
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Phase 2 Final Review (Completed)
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
        <Accordion defaultExpanded sx={{ mb: 2 }}>
          <AccordionSummary expandIcon={<ExpandMore />}>
            <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
              Entity Attributes
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            {entityNames.map((entityName) => {
              const attrs = attributes[entityName] || [];
              
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
            {Object.keys(relationAttributes).length > 0 ? (
              <List>
                {Object.entries(relationAttributes).map(([relationId, relationData]: [string, any]) => {
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
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="phase2_final" />}
    </Box>
  );
};

export default Phase2FinalCheckpointReadOnly;
