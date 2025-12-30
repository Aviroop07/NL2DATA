import React from 'react';
import { Box, LinearProgress, Typography } from '@mui/material';
import { useAppStore } from '../../stores/useAppStore';

const ProgressIndicator: React.FC = () => {
  const { progress, currentPhase, currentStep } = useAppStore();
  
  return (
    <Box sx={{ width: '100%', mb: 2 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
        <Typography variant="body2">
          Progress: {Math.round(progress)}%
        </Typography>
        {currentPhase && (
          <Typography variant="body2" color="text.secondary">
            Phase {currentPhase} - {currentStep || 'Starting...'}
          </Typography>
        )}
      </Box>
      <LinearProgress variant="determinate" value={progress} />
    </Box>
  );
};

export default ProgressIndicator;

