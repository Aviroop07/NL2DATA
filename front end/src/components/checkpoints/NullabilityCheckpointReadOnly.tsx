import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List, ListItem, Chip } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

interface NullabilityCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const NullabilityCheckpointReadOnly: React.FC<NullabilityCheckpointReadOnlyProps> = ({ data, justification }) => {
  const { completedCheckpoints } = useAppStore();
  const nullability = data.nullability || {};
  
  // Get attributes from data first, then from completed checkpoints
  const attributesCheckpoint = completedCheckpoints.find(c => c.type === "attributes");
  const attributes = data.attributes || attributesCheckpoint?.data?.attributes || {};
  
  // Get all entity names from attributes (to show all entities, not just those with nullability data)
  const entityNamesFromAttributes = Object.keys(attributes);
  const entityNamesFromNullability = Object.keys(nullability);
  // Combine and deduplicate
  const allEntityNames = Array.from(new Set([...entityNamesFromAttributes, ...entityNamesFromNullability]));
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Nullability Constraints Review (Completed)
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
          const entityData = nullability[entityName] || { nullable: [], non_nullable: [] };
          const nullable = entityData.nullable || [];
          const nonNullable = entityData.non_nullable || [];
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
                      const isNullable = nullable.includes(attrName);
                      const attrReasoning = reasoning[attrName] || '';
                      
                      return (
                        <ListItem key={attrName} sx={{ py: 0.5, borderBottom: '1px solid', borderColor: 'divider' }}>
                          <Chip 
                            label={attrName} 
                            size="small" 
                            sx={{ mr: 1 }} 
                          />
                          <Chip 
                            label={isNullable ? "Nullable" : "NOT NULL"} 
                            size="small" 
                            color={isNullable ? "default" : "error"}
                            variant="outlined"
                            sx={{ mr: 1 }}
                          />
                          {attrReasoning && (
                            <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
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
      
      {justification && <JustificationDisplay justification={justification} type="nullability" />}
    </Box>
  );
};

export default NullabilityCheckpointReadOnly;
