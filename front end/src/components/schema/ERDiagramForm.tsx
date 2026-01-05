import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  Chip,
  Divider
} from '@mui/material';
import { ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';

const ERDiagramForm: React.FC = () => {
  const { erDiagram } = useAppStore();
  
  if (!erDiagram) {
    return (
      <Box sx={{ textAlign: 'center', py: 4 }}>
        <Typography variant="body2" color="text.secondary">
          No ER diagram data available
        </Typography>
      </Box>
    );
  }
  
  const entities = erDiagram.entities || [];
  const relations = erDiagram.relations || [];
  
  return (
    <Box>
      {/* Entities Section */}
      {entities.length > 0 && (
        <Box sx={{ mb: 3 }}>
          <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
            Entities ({entities.length})
          </Typography>
          {entities.map((entity: any, index: number) => {
            const entityName = entity.name || entity;
            const entityDesc = entity.description || '';
            const attributes = entity.attributes || [];
            const primaryKey = entity.primary_key || [];
            
            return (
              <Accordion key={index} sx={{ mb: 1 }}>
                <AccordionSummary expandIcon={<ExpandMore />}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                      {entityName}
                    </Typography>
                    {primaryKey.length > 0 && (
                      <Chip 
                        label={`PK: ${primaryKey.join(', ')}`} 
                        size="small" 
                        color="primary" 
                        variant="outlined"
                      />
                    )}
                  </Box>
                </AccordionSummary>
                <AccordionDetails>
                  {entityDesc && (
                    <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                      {entityDesc}
                    </Typography>
                  )}
                  {attributes.length > 0 ? (
                    <Box>
                      <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
                        Attributes ({attributes.length}):
                      </Typography>
                      <List dense>
                        {attributes.map((attr: any, attrIndex: number) => {
                          const attrName = attr.name || attr;
                          const attrDesc = attr.description || '';
                          const attrType = attr.type_hint || attr.data_type || '';
                          const isPK = primaryKey.includes(attrName);
                          
                          return (
                            <ListItem key={attrIndex} sx={{ py: 0.5, pl: 2 }}>
                              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                                <Typography variant="body2" sx={{ fontWeight: isPK ? 600 : 400 }}>
                                  {attrName}
                                </Typography>
                                {isPK && (
                                  <Chip label="PK" size="small" color="primary" sx={{ height: 20 }} />
                                )}
                                {attrType && (
                                  <Chip 
                                    label={attrType} 
                                    size="small" 
                                    variant="outlined"
                                    sx={{ height: 20 }}
                                  />
                                )}
                                {attrDesc && (
                                  <Typography variant="caption" color="text.secondary" sx={{ ml: 'auto' }}>
                                    {attrDesc}
                                  </Typography>
                                )}
                              </Box>
                            </ListItem>
                          );
                        })}
                      </List>
                    </Box>
                  ) : (
                    <Typography variant="body2" color="text.secondary">
                      No attributes defined
                    </Typography>
                  )}
                </AccordionDetails>
              </Accordion>
            );
          })}
        </Box>
      )}
      
      {/* Relations Section */}
      {relations.length > 0 && (
        <Box>
          <Typography variant="h6" sx={{ fontWeight: 600, mb: 2 }}>
            Relations ({relations.length})
          </Typography>
          {relations.map((relation: any, index: number) => {
            const relationEntities = relation.entities || [];
            const relationType = relation.type || '';
            const relationDesc = relation.description || '';
            const entityCardinalities = relation.entity_cardinalities || {};
            
            return (
              <Paper 
                key={index}
                elevation={0}
                sx={{ 
                  p: 2, 
                  mb: 2,
                  border: '1px solid',
                  borderColor: 'divider',
                  borderRadius: 1
                }}
              >
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                  <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                    {relationEntities.join(' ↔ ')}
                  </Typography>
                  {relationType && (
                    <Chip 
                      label={relationType} 
                      size="small" 
                      color="secondary" 
                      variant="outlined"
                    />
                  )}
                </Box>
                {relationDesc && (
                  <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                    {relationDesc}
                  </Typography>
                )}
                {Object.keys(entityCardinalities).length > 0 && (
                  <Box sx={{ mt: 1 }}>
                    <Typography variant="caption" color="text.secondary">
                      Cardinalities: {Object.entries(entityCardinalities).map(([entity, card]) => 
                        `${entity}: ${card}`
                      ).join(', ')}
                    </Typography>
                  </Box>
                )}
              </Paper>
            );
          })}
        </Box>
      )}
      
      {entities.length === 0 && relations.length === 0 && (
        <Box sx={{ textAlign: 'center', py: 4 }}>
          <Typography variant="body2" color="text.secondary">
            No ER diagram data available
          </Typography>
        </Box>
      )}
    </Box>
  );
};

export default ERDiagramForm;
