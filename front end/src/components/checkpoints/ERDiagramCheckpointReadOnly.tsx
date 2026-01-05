import React from 'react';
import { Box, Typography, Paper } from '@mui/material';
import { Lock } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';
import { API_BASE_URL } from '../../utils/constants';

interface ERDiagramCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const ERDiagramCheckpointReadOnly: React.FC<ERDiagramCheckpointReadOnlyProps> = ({ data, justification }) => {
  // Get image URL and prepend API base URL if it's a relative path
  const rawImageUrl = data?.er_design?.imageUrl;
  const imageUrl = rawImageUrl 
    ? (rawImageUrl.startsWith('/') && !rawImageUrl.startsWith('//') 
        ? `${API_BASE_URL}${rawImageUrl}` 
        : rawImageUrl)
    : null;

  // Debug logging
  React.useEffect(() => {
    if (!imageUrl) {
      console.log('ERDiagramCheckpointReadOnly: No imageUrl found', {
        hasData: !!data,
        hasErDesign: !!data?.er_design,
        imageUrl: data?.er_design?.imageUrl,
        fullData: data
      });
    } else {
      console.log('ERDiagramCheckpointReadOnly: Image URL found', imageUrl);
    }
  }, [imageUrl, data]);

  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          ER Diagram Review (Completed)
        </Typography>
      </Box>
      
      <Paper 
        elevation={0}
        sx={{ 
          p: 3, 
          mb: 3,
          bgcolor: 'grey.50',
          border: '1px solid',
          borderColor: 'divider',
          textAlign: 'center'
        }}
      >
        {imageUrl ? (
          <img
            src={imageUrl}
            alt="ER Diagram"
            style={{ 
              maxWidth: '100%', 
              height: 'auto',
              borderRadius: 8
            }}
          />
        ) : (
          <Typography variant="body2" color="text.secondary">
            ER Diagram image not available
          </Typography>
        )}
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="er_diagram" />}
    </Box>
  );
};

export default ERDiagramCheckpointReadOnly;
