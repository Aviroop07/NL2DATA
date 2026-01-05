import React from 'react';
import { Box, Typography, Paper, Chip } from '@mui/material';
import { Lock } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface DomainCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const DomainCheckpointReadOnly: React.FC<DomainCheckpointReadOnlyProps> = ({ data, justification }) => {
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Domain Review (Completed)
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
        <Typography variant="subtitle2" sx={{ mb: 1, color: 'text.secondary' }}>
          Domain
        </Typography>
        <Typography variant="h6" sx={{ fontWeight: 600 }}>
          {data.domain || 'N/A'}
        </Typography>
        {data.has_explicit_domain !== undefined && (
          <Chip 
            label={data.has_explicit_domain ? "Explicitly mentioned" : "Inferred"}
            size="small"
            color={data.has_explicit_domain ? "primary" : "default"}
            sx={{ mt: 1 }}
          />
        )}
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="domain" />}
    </Box>
  );
};

export default DomainCheckpointReadOnly;
