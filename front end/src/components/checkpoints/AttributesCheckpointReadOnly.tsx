import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface AttributesCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const AttributesCheckpointReadOnly: React.FC<AttributesCheckpointReadOnlyProps> = ({ data, justification }) => {
  const attributes = data.attributes || {};
  const entityNames = Object.keys(attributes);
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Attributes Review (Completed)
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
          const attrs = attributes[entityName] || [];
          return (
            <Accordion key={entityName} sx={{ mb: 1 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName} ({attrs.length} attributes)
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                <List>
                  {attrs.map((attr: any, index: number) => (
                    <Box
                      key={index}
                      sx={{
                        border: '1px solid',
                        borderColor: 'divider',
                        borderRadius: 1,
                        mb: 1,
                        p: 1,
                        bgcolor: 'background.paper'
                      }}
                    >
                      <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                        {attr.name}
                      </Typography>
                      <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.5 }}>
                        {attr.data_type && `Type: ${attr.data_type}`} {attr.description ? `- ${attr.description}` : ''}
                      </Typography>
                    </Box>
                  ))}
                </List>
              </AccordionDetails>
            </Accordion>
          );
        })}
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="attributes" />}
    </Box>
  );
};

export default AttributesCheckpointReadOnly;
