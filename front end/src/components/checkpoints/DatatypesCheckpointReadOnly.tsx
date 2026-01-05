import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List, ListItem, Chip } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface DatatypesCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const DatatypesCheckpointReadOnly: React.FC<DatatypesCheckpointReadOnlyProps> = ({ data, justification }) => {
  const dataTypes = data.data_types || {};
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Data Type Assignment Review (Completed)
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
        <Typography variant="subtitle2" sx={{ mb: 2, color: 'text.secondary' }}>
          {Object.keys(dataTypes).length} entities with assigned data types
        </Typography>
        
        {Object.entries(dataTypes).map(([entityName, entityData]) => {
          const attributeTypes = entityData?.attribute_types || entityData || {};
          
          return (
            <Accordion key={entityName} sx={{ mb: 1 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName}
                </Typography>
                <Chip 
                  label={`${Object.keys(attributeTypes).length} attributes`} 
                  size="small" 
                  sx={{ ml: 2 }}
                />
              </AccordionSummary>
              <AccordionDetails>
                <List dense>
                  {Object.entries(attributeTypes).map(([attrName, typeInfo]: [string, any]) => {
                    const type = typeInfo?.type || '';
                    const size = typeInfo?.size;
                    const precision = typeInfo?.precision;
                    const scale = typeInfo?.scale;
                    const reasoning = typeInfo?.reasoning || '';
                    
                    // Build type string
                    let typeString = type;
                    if (type && (type.toUpperCase() === "VARCHAR" || type.toUpperCase() === "CHAR")) {
                      typeString = size ? `${type}(${size})` : `${type}(255)`;
                    } else if (type && (type.toUpperCase() === "DECIMAL" || type.toUpperCase() === "NUMERIC")) {
                      if (precision !== null && precision !== undefined && scale !== null && scale !== undefined) {
                        typeString = `${type}(${precision},${scale})`;
                      } else if (precision !== null && precision !== undefined) {
                        typeString = `${type}(${precision})`;
                      }
                    }
                    
                    return (
                      <ListItem key={attrName} sx={{ flexDirection: 'column', alignItems: 'stretch', py: 1.5 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 1 }}>
                          <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 150 }}>
                            {attrName}:
                          </Typography>
                          <Chip 
                            label={typeString || 'Not assigned'} 
                            size="small" 
                            color="primary"
                            variant="outlined"
                          />
                        </Box>
                        {reasoning && (
                          <Typography variant="caption" color="text.secondary" sx={{ pl: 2 }}>
                            {reasoning}
                          </Typography>
                        )}
                      </ListItem>
                    );
                  })}
                </List>
              </AccordionDetails>
            </Accordion>
          );
        })}
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="datatypes" />}
    </Box>
  );
};

export default DatatypesCheckpointReadOnly;
