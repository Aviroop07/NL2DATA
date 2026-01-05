import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List, ListItem, Chip } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface CheckConstraintsCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const CheckConstraintsCheckpointReadOnly: React.FC<CheckConstraintsCheckpointReadOnlyProps> = ({ data, justification }) => {
  const checkConstraints = data.check_constraints || {};
  
  const entityNames = Object.keys(checkConstraints);
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Check Constraints Review (Completed)
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
        {entityNames.map((entityName) => {
          const entityConstraints = checkConstraints[entityName] || {};
          const constraintKeys = Object.keys(entityConstraints);
          
          return (
            <Accordion key={entityName} sx={{ mb: 2 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName}
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                {constraintKeys.length > 0 ? (
                  <List>
                    {constraintKeys.map((attrName: string) => {
                      const constraint = entityConstraints[attrName] || {};
                      
                      return (
                        <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1 }}>
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                            <Chip label={attrName} size="small" sx={{ mr: 1 }} />
                            {constraint.condition && (
                              <Typography variant="body2" sx={{ fontFamily: 'monospace', bgcolor: 'grey.100', px: 1, py: 0.5, borderRadius: 1 }}>
                                {constraint.condition}
                              </Typography>
                            )}
                          </Box>
                          {constraint.description && (
                            <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5, ml: 1 }}>
                              {constraint.description}
                            </Typography>
                          )}
                          {constraint.reasoning && (
                            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, ml: 1 }}>
                              Reasoning: {constraint.reasoning}
                            </Typography>
                          )}
                        </ListItem>
                      );
                    })}
                  </List>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No check constraints specified for this entity.
                  </Typography>
                )}
              </AccordionDetails>
            </Accordion>
          );
        })}
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="check_constraints" />}
    </Box>
  );
};

export default CheckConstraintsCheckpointReadOnly;
