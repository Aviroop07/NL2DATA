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

interface GenerationStrategiesCheckpointReadOnlyProps {
  data: {
    generation_strategies?: Record<string, Record<string, any>>;
  };
  justification?: Record<string, any>;
}

const GenerationStrategiesCheckpointReadOnly: React.FC<GenerationStrategiesCheckpointReadOnlyProps> = ({ data, justification }) => {
  const generationStrategies = data.generation_strategies || {};
  const entityNames = Object.keys(generationStrategies);
  const totalStrategies = Object.values(generationStrategies).reduce(
    (sum, attrs) => sum + Object.keys(attrs).length, 
    0
  );
  
  const getStrategyType = (strategy: Record<string, any>): string => {
    if (strategy.distribution) return 'Distribution';
    if (strategy.generator) return 'Generator';
    if (strategy.formula) return 'Formula';
    if (strategy.range) return 'Range';
    return 'Custom';
  };
  
  return (
    <Box>
      <Typography variant="h6" gutterBottom>
        Generation Strategies (Read-Only)
      </Typography>
      
      {justification && (
        <Box sx={{ mb: 2 }}>
          <JustificationDisplay justification={justification} />
        </Box>
      )}
      
      <Paper sx={{ p: 2 }}>
        <Typography variant="subtitle1" sx={{ mb: 2, fontWeight: 'bold' }}>
          Generation Strategies ({totalStrategies} strategies across {entityNames.length} entities)
        </Typography>
        
        {entityNames.length === 0 ? (
          <Typography variant="body2" color="text.secondary">
            No generation strategies found.
          </Typography>
        ) : (
          entityNames.map((entityName) => {
            const entityStrategies = generationStrategies[entityName] || {};
            const attributeNames = Object.keys(entityStrategies);
            
            if (attributeNames.length === 0) return null;
            
            return (
              <Accordion key={entityName}>
                <AccordionSummary expandIcon={<ExpandMore />}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                    <Typography variant="subtitle2">
                      {entityName}
                    </Typography>
                    <Chip label={attributeNames.length} size="small" />
                  </Box>
                </AccordionSummary>
                <AccordionDetails>
                  <List>
                    {attributeNames.map((attrName) => {
                      const strategy = entityStrategies[attrName];
                      const strategyType = getStrategyType(strategy);
                      
                      return (
                        <ListItem key={attrName}>
                          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
                            <Typography variant="body2">
                              {attrName}
                            </Typography>
                            <Chip label={strategyType} size="small" color="primary" />
                          </Box>
                        </ListItem>
                      );
                    })}
                  </List>
                </AccordionDetails>
              </Accordion>
            );
          })
        )}
      </Paper>
    </Box>
  );
};

export default GenerationStrategiesCheckpointReadOnly;
