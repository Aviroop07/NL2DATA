import React from 'react';
import { Box, Typography, Paper } from '@mui/material';
import { Lock } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';
import RelationalSchemaEditor from '../schema/RelationalSchemaEditor';

interface RelationalSchemaCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const RelationalSchemaCheckpointReadOnly: React.FC<RelationalSchemaCheckpointReadOnlyProps> = ({ data, justification }) => {
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Relational Schema Review (Completed)
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
        <RelationalSchemaEditor />
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="relational_schema" />}
    </Box>
  );
};

export default RelationalSchemaCheckpointReadOnly;
