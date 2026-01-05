import React from 'react';
import { Box, Typography, Paper, Accordion, AccordionSummary, AccordionDetails, List, ListItem, Chip } from '@mui/material';
import { Lock, ExpandMore } from '@mui/icons-material';
import JustificationDisplay from './JustificationDisplay';

interface MultivaluedDerivedCheckpointReadOnlyProps {
  data: Record<string, any>;
  justification: Record<string, any> | null;
}

const MultivaluedDerivedCheckpointReadOnly: React.FC<MultivaluedDerivedCheckpointReadOnlyProps> = ({ data, justification }) => {
  const multivaluedDerived = data.multivalued_derived || {};
  const derivedFormulas = data.derived_formulas || {};
  const attributes = data.attributes || {};
  
  // Get all entity names from attributes (to show all entities, not just those with multivalued/derived)
  const entityNamesFromAttributes = Object.keys(attributes);
  const entityNamesFromMultivalued = Object.keys(multivaluedDerived);
  // Combine and deduplicate
  const allEntityNames = Array.from(new Set([...entityNamesFromAttributes, ...entityNamesFromMultivalued]));
  
  return (
    <Box>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
        <Lock fontSize="small" color="disabled" />
        <Typography variant="h5" sx={{ fontWeight: 600, color: 'text.secondary' }}>
          Multivalued and Derived Attributes Review (Completed)
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
        {allEntityNames.map((entityName) => {
          const entityData = multivaluedDerived[entityName] || {};
          const multivalued = entityData.multivalued || [];
          const derived = entityData.derived || [];
          const multivaluedHandling = entityData.multivalued_handling || {};
          
          // Get all attributes for this entity
          const allAttributes = attributes[entityName] || [];
          const attributeNames = allAttributes.map((a: any) => a.name || a);
          
          return (
            <Accordion key={entityName} defaultExpanded sx={{ mb: 2 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName} {attributeNames.length > 0 && `(${attributeNames.length} attributes)`}
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                {attributeNames.length > 0 ? (
                  <List>
                    {attributeNames.map((attrName: string) => {
                      const isMultivalued = multivalued.includes(attrName);
                      const isDerived = derived.includes(attrName);
                      const formulaKey = `${entityName}.${attrName}`;
                      const formulaInfo = derivedFormulas[formulaKey] || {};
                      const formula = formulaInfo.formula || '';
                      
                      return (
                        <ListItem 
                          key={attrName} 
                          sx={{ 
                            flexDirection: 'column', 
                            alignItems: 'stretch', 
                            py: 1.5,
                            borderBottom: '1px solid',
                            borderColor: 'divider'
                          }}
                        >
                          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: isDerived && formula ? 1 : 0 }}>
                            <Typography variant="body2" sx={{ fontWeight: 600, minWidth: 120 }}>
                              {attrName}
                            </Typography>
                            <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                              {isMultivalued && (
                                <Chip label="Multivalued" size="small" color="primary" />
                              )}
                              {isDerived && (
                                <Chip label="Derived" size="small" color="secondary" />
                              )}
                              {!isMultivalued && !isDerived && (
                                <Chip label="Regular" size="small" variant="outlined" />
                              )}
                              {isMultivalued && multivaluedHandling[attrName] && (
                                <Chip 
                                  label={`Handling: ${multivaluedHandling[attrName]}`} 
                                  size="small" 
                                  variant="outlined" 
                                />
                              )}
                            </Box>
                          </Box>
                          
                          {isDerived && formula && (
                            <Box sx={{ mt: 1 }}>
                              <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mb: 0.5 }}>
                                DSL Formula:
                              </Typography>
                              <Typography variant="body2" sx={{ fontFamily: 'monospace', bgcolor: 'grey.100', p: 1, borderRadius: 1 }}>
                                {formula}
                              </Typography>
                              {formulaInfo.dependencies && formulaInfo.dependencies.length > 0 && (
                                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
                                  Dependencies: {formulaInfo.dependencies.join(', ')}
                                </Typography>
                              )}
                            </Box>
                          )}
                        </ListItem>
                      );
                    })}
                  </List>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    No attributes found for this entity.
                  </Typography>
                )}
              </AccordionDetails>
            </Accordion>
          );
        })}
      </Paper>
      
      {justification && <JustificationDisplay justification={justification} type="multivalued_derived" />}
    </Box>
  );
};

export default MultivaluedDerivedCheckpointReadOnly;
