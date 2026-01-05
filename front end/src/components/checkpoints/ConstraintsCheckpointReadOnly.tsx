import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Chip,
  List,
  ListItem
} from '@mui/material';
import { ExpandMore } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface ConstraintsCheckpointReadOnlyProps {
  data: {
    constraints?: Array<Record<string, any>>;
  };
  justification?: Record<string, any>;
}

const ConstraintsCheckpointReadOnly: React.FC<ConstraintsCheckpointReadOnlyProps> = ({ data, justification }) => {
  const constraints = data.constraints || [];
  
  // Group constraints by category
  const constraintsByCategory = constraints.reduce((acc, constraint) => {
    const category = constraint.constraint_category || constraint.category || 'other_constraints';
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(constraint);
    return acc;
  }, {} as Record<string, Array<Record<string, any>>>);
  
  const categoryLabels: Record<string, string> = {
    statistical_constraints: 'Statistical Constraints',
    structural_constraints: 'Structural Constraints',
    distribution_constraints: 'Distribution Constraints',
    other_constraints: 'Other Constraints'
  };
  
  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Constraints (Read-Only)
      </Typography>
      
      {justification && (
        <Box sx={{ mb: 2 }}>
          <JustificationDisplay justification={justification} />
        </Box>
      )}
      
      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 'bold' }}>
          Constraints ({constraints.length} total)
        </Typography>
        
        {constraints.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            No constraints found.
          </Typography>
        ) : (
          Object.entries(constraintsByCategory).map(([category, categoryConstraints]) => (
            <Accordion key={category}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                  <Typography variant="subtitle2">
                    {categoryLabels[category] || category}
                  </Typography>
                  <Chip label={categoryConstraints.length} size="small" />
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <List>
                  {categoryConstraints.map((constraint, index) => (
                    <ListItem key={index} sx={{ flexDirection: 'column', alignItems: 'stretch' }}>
                      <Typography variant="body2" sx={{ fontWeight: 'bold', mb: 0.5 }}>
                        {constraint.description || 'No description'}
                      </Typography>
                      {constraint.enforcement_strategy && (
                        <Chip 
                          label={`Enforcement: ${constraint.enforcement_strategy}`} 
                          size="small" 
                          color="primary"
                          sx={{ mt: 0.5 }}
                        />
                      )}
                    </ListItem>
                  ))}
                </List>
              </AccordionDetails>
            </Accordion>
          ))
        )}
      </Paper>
    </Box>
  );
};

export default ConstraintsCheckpointReadOnly;
