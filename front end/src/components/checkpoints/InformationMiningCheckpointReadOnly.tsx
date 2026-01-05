import React from 'react';
import {
  Box,
  Typography,
  Paper,
  List,
  ListItem,
  Chip,
  Accordion,
  AccordionSummary,
  AccordionDetails
} from '@mui/material';
import { ExpandMore, CheckCircle, Error as ErrorIcon } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface InformationMiningCheckpointReadOnlyProps {
  data: {
    information_needs?: Array<{
      description: string;
      sql_query: string;
      entities_involved?: string[];
      is_valid?: boolean;
      validation_error?: string;
    }>;
  };
  justification?: Record<string, any>;
}

const InformationMiningCheckpointReadOnly: React.FC<InformationMiningCheckpointReadOnlyProps> = ({ data, justification }) => {
  const informationNeeds = data.information_needs || [];
  
  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Information Mining (Read-Only)
      </Typography>
      
      {justification && (
        <Box sx={{ mb: 2 }}>
          <JustificationDisplay justification={justification} />
        </Box>
      )}
      
      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 'bold' }}>
          Information Needs ({informationNeeds.length})
        </Typography>
        
        {informationNeeds.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            No information needs found.
          </Typography>
        ) : (
          <List>
            {informationNeeds.map((need, index) => (
              <ListItem key={index} sx={{ flexDirection: 'column', alignItems: 'stretch' }}>
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                    <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                      #{index + 1}: {need.description}
                    </Typography>
                    {need.is_valid === true && (
                      <Chip icon={<CheckCircle />} label="Valid" color="success" size="small" />
                    )}
                    {need.is_valid === false && (
                      <Chip icon={<ErrorIcon />} label="Invalid" color="error" size="small" />
                    )}
                  </Box>
                  
                  <Typography variant="caption" component="pre" sx={{ 
                    bgcolor: 'grey.100', 
                    p: 1, 
                    borderRadius: 1,
                    overflow: 'auto',
                    fontSize: '0.75rem',
                    fontFamily: 'monospace'
                  }}>
                    {need.sql_query}
                  </Typography>
                  
                  {need.entities_involved && need.entities_involved.length > 0 && (
                    <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                      Entities: {need.entities_involved.join(', ')}
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

export default InformationMiningCheckpointReadOnly;
