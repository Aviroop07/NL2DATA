import React from 'react';
import {
  Box,
  Typography,
  Paper,
  List,
  ListItem,
  Chip
} from '@mui/material';
import JustificationDisplay from './JustificationDisplay';

interface FunctionalDependenciesCheckpointReadOnlyProps {
  data: {
    functional_dependencies?: Array<{
      lhs: string[];
      rhs: string[];
      reasoning?: string;
    }>;
  };
  justification?: Record<string, any>;
}

const FunctionalDependenciesCheckpointReadOnly: React.FC<FunctionalDependenciesCheckpointReadOnlyProps> = ({ data, justification }) => {
  const functionalDependencies = data.functional_dependencies || [];
  
  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Functional Dependencies (Read-Only)
      </Typography>
      
      {justification && (
        <Box sx={{ mb: 2 }}>
          <JustificationDisplay justification={justification} />
        </Box>
      )}
      
      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 'bold' }}>
          Functional Dependencies ({functionalDependencies.length})
        </Typography>
        
        {functionalDependencies.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            No functional dependencies found.
          </Typography>
        ) : (
          <List>
            {functionalDependencies.map((fd, index) => (
              <ListItem key={index} sx={{ flexDirection: 'column', alignItems: 'stretch' }}>
                <Box sx={{ width: '100%' }}>
                  <Typography variant="body2" sx={{ mb: 1 }}>
                    <strong>FD #{index + 1}:</strong>
                  </Typography>
                  
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1, flexWrap: 'wrap' }}>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {fd.lhs.map((attr, i) => (
                        <Chip key={i} label={attr} size="small" variant="outlined" />
                      ))}
                    </Box>
                    <Typography variant="h6" sx={{ mx: 1 }}>
                      →
                    </Typography>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {fd.rhs.map((attr, i) => (
                        <Chip key={i} label={attr} size="small" color="primary" />
                      ))}
                    </Box>
                  </Box>
                  
                  {fd.reasoning && (
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontStyle: 'italic' }}>
                      Reasoning: {fd.reasoning}
                    </Typography>
                  )}
                </Box>
              </ListItem>
            ))}
          </List>
        )}
      </Paper>
    </Box>
  );
};

export default FunctionalDependenciesCheckpointReadOnly;
