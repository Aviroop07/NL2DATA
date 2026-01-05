import React from 'react';
import { Box, Paper, Container } from '@mui/material';
import NLInputArea from '../nl-input/NLInputArea';
import CheckpointReview from '../checkpoints/CheckpointReview';

const MainPanel: React.FC = () => {
  return (
    <Box 
      sx={{ 
        flex: 1, 
        display: 'flex', 
        flexDirection: 'column', 
        overflow: 'auto',
        p: 3,
        gap: 3
      }}
    >
      <Container maxWidth="lg" sx={{ width: '100%', p: 0 }}>
        <Paper 
          elevation={0}
          sx={{ 
            p: 0, 
            mb: 3,
            border: '1px solid',
            borderColor: 'divider',
            overflow: 'hidden'
          }}
        >
          <NLInputArea />
        </Paper>
        
        <CheckpointReview />
      </Container>
    </Box>
  );
};

export default MainPanel;

