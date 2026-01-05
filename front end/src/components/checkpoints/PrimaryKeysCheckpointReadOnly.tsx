import React from 'react';
import { Box, Typography, Paper, List, ListItem, Chip } from '@mui/material';
import { Lock } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface PrimaryKeysCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const PrimaryKeysCheckpointReadOnly: React.FC<PrimaryKeysCheckpointReadOnlyProps> = ({ data, justification }) => {
  const primaryKeys = data.primary_keys || {};
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Primary Keys Review (Completed)
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
        <List>
          {Object.entries(primaryKeys).map(([entityName, pk]) => (
            <ListItem
              key={entityName}
              sx={{
                border: '1px solid',
                borderColor: 'divider',
                borderRadius: 1,
                mb: 1,
                bgcolor: 'background.paper'
              }}
            >
              <Box sx={{ width: '100%' }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600, mb: 1 }}>
                  {entityName}
                </Typography>
                <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                  {(pk as string[]).map((attr) => (
                    <Chip key={attr} label={attr} size="small" color="primary" />
                  ))}
                </Box>
              </Box>
            </ListItem>
          ))}
        </List>
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="primary_keys" />}
    </Box>
  );
};

export default PrimaryKeysCheckpointReadOnly;
