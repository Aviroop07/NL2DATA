import React from 'react';
import { Box, Typography, Paper, List, ListItem } from '@mui/material';
import { Lock } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface EntitiesCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const EntitiesCheckpointReadOnly: React.FC<EntitiesCheckpointReadOnlyProps> = ({ data, justification }) => {
  const entities = data.entities || [];
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Entities Review (Completed)
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
        <Typography variant="subtitle2" sx={{ mb: 2, color: 'text.secondary' }}>
          {entities.length} entities identified
        </Typography>
        
        <List>
          {entities.map((entity: any, index: number) => (
            <ListItem
              key={index}
              sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                mb: 1,
                bgcolor: 'background.paper'
              }}
            >
              <Box sx={{ width: '100%' }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entity.name || entity.name}
                </Typography>
                <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.5 }}>
                  {entity.description || 'No description'}
                </Typography>
              </Box>
            </ListItem>
          ))}
        </List>
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="entities" />}
    </Box>
  );
};

export default EntitiesCheckpointReadOnly;
