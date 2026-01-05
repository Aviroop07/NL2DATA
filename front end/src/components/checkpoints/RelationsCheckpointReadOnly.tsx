import React from 'react';
import { Box, Typography, Paper, List, ListItem, Chip } from '@mui/material';
import { Lock } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface RelationsCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const RelationsCheckpointReadOnly: React.FC<RelationsCheckpointReadOnlyProps> = ({ data, justification }) => {
  const relations = data.relations || [];
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Relations Review (Completed)
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
          {relations.length} relations identified
        </Typography>
        
        <List>
          {relations.map((relation: any, index: number) => {
            const entityCardinalities = relation.entity_cardinalities || {};
            const entityParticipations = relation.entity_participations || {};
            
            return (
              <ListItem
                key={index}
                sx={{
                  border: '1px solid',
                  borderColor: 'divider',
                  borderRadius: 1,
                  mb: 1,
                  bgcolor: 'background.paper',
                  flexDirection: 'column',
                  alignItems: 'stretch'
                }}
              >
                <Box sx={{ width: '100%' }}>
                  <Box sx={{ mb: 1 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                      {relation.entities?.join(' ↔ ') || 'Unknown'}
                    </Typography>
                    <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.5 }}>
                      Type: {relation.type || 'N/A'} {relation.description ? `- ${relation.description}` : ''}
                    </Typography>
                  </Box>
                  
                  {/* Entity Cardinalities and Participations */}
                  {relation.entities && relation.entities.length > 0 && (
                    <Box sx={{ mt: 2, display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                      {relation.entities.map((entityName: string) => {
                        const cardinality = entityCardinalities[entityName] || "1";
                        const participation = entityParticipations[entityName] || "partial";
                        
                        return (
                          <Box 
                            key={entityName}
                            sx={{ 
                              display: 'flex', 
                              alignItems: 'center', 
                              gap: 2,
                              p: 1.5,
                              bgcolor: 'grey.50',
                              borderRadius: 1,
                              border: '1px solid',
                              borderColor: 'divider'
                            }}
                          >
                            <Typography variant="body2" sx={{ minWidth: 120, fontWeight: 500 }}>
                              {entityName}:
                            </Typography>
                            
                            {/* Cardinality Display */}
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                                Cardinality:
                              </Typography>
                              <Chip 
                                label={cardinality} 
                                size="small" 
                                color={cardinality === "1" ? "primary" : "secondary"}
                                variant="outlined"
                                sx={{ height: 24 }}
                              />
                            </Box>
                            
                            {/* Participation Display */}
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                                Participation:
                              </Typography>
                              <Chip 
                                label={participation === "total" ? "Total" : "Partial"} 
                                size="small" 
                                color={participation === "total" ? "success" : "default"}
                                variant="outlined"
                                sx={{ height: 24 }}
                              />
                            </Box>
                          </Box>
                        );
                      })}
                    </Box>
                  )}
                </Box>
              </ListItem>
            );
          })}
        </List>
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="relations" />}
    </Box>
  );
};

export default RelationsCheckpointReadOnly;
