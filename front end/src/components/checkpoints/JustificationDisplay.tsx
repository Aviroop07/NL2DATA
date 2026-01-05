import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Chip,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  ListItemText
} from '@mui/material';
import { ExpandMore, CheckCircle, Info, Lightbulb } from '@mui/icons-material';

interface JustificationDisplayProps {
  justification: Record<string, any>;
  type: 'domain' | 'entities' | 'relations' | 'attributes' | 'primary_keys' | 'multivalued_derived' | 'nullability' | 'default_values' | 'check_constraints' | 'phase2_final' | 'er_diagram' | 'relational_schema';
}

const JustificationDisplay: React.FC<JustificationDisplayProps> = ({ justification, type }) => {
  if (!justification || Object.keys(justification).length === 0) {
    return null;
  }

  const renderDomainJustification = () => {
    const step1_1 = justification.step_1_1;
    const step1_3 = justification.step_1_3;

    return (
      <Box>
        {step1_1 && (
          <Paper elevation={0} sx={{ p: 2, mb: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <CheckCircle color="primary" fontSize="small" />
              <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                Domain Detection (Step 1.1)
              </Typography>
            </Box>
            {step1_1.has_explicit_domain ? (
              <Box>
                <Typography variant="body2" sx={{ mb: 1 }}>
                  <strong>Status:</strong> Domain was explicitly mentioned in the description
                </Typography>
                {step1_1.evidence && (
                  <Box sx={{ mb: 1 }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                      Evidence:
                    </Typography>
                    <Chip 
                      label={step1_1.evidence} 
                      size="small" 
                      color="primary" 
                      variant="outlined"
                      sx={{ fontStyle: 'italic' }}
                    />
                  </Box>
                )}
                {step1_1.reasoning && (
                  <Box>
                    <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                      Reasoning:
                    </Typography>
                    <Typography variant="body2" sx={{ color: 'text.secondary', fontStyle: 'italic' }}>
                      {step1_1.reasoning}
                    </Typography>
                  </Box>
                )}
              </Box>
            ) : (
              <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                No explicit domain was found in the description.
              </Typography>
            )}
          </Paper>
        )}

        {step1_3 && (
          <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
              <Lightbulb color="warning" fontSize="small" />
              <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                Domain Inference (Step 1.3)
              </Typography>
            </Box>
            <Box sx={{ mb: 1 }}>
              <Typography variant="body2">
                <strong>Inferred Domain:</strong> {step1_3.primary_domain || 'N/A'}
              </Typography>
              {step1_3.confidence !== undefined && (
                <Box sx={{ mt: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    Confidence:
                  </Typography>
                  <Chip 
                    label={`${(step1_3.confidence * 100).toFixed(0)}%`}
                    size="small"
                    color={step1_3.confidence > 0.7 ? 'success' : step1_3.confidence > 0.4 ? 'warning' : 'default'}
                  />
                </Box>
              )}
            </Box>
            {step1_3.evidence && Array.isArray(step1_3.evidence) && step1_3.evidence.length > 0 && (
              <Box sx={{ mb: 1 }}>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                  Supporting Evidence:
                </Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {step1_3.evidence.map((ev: string, idx: number) => (
                    <Chip 
                      key={idx}
                      label={ev} 
                      size="small" 
                      color="primary" 
                      variant="outlined"
                      sx={{ fontStyle: 'italic' }}
                    />
                  ))}
                </Box>
              </Box>
            )}
            {step1_3.reasoning && (
              <Box>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                  Reasoning:
                </Typography>
                <Typography variant="body2" sx={{ color: 'text.secondary', fontStyle: 'italic' }}>
                  {step1_3.reasoning}
                </Typography>
              </Box>
            )}
            {step1_3.alternatives && Array.isArray(step1_3.alternatives) && step1_3.alternatives.length > 0 && (
              <Box sx={{ mt: 1 }}>
                <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                  Alternative Domains Considered:
                </Typography>
                <List dense>
                  {step1_3.alternatives.map((alt: any, idx: number) => (
                    <ListItem key={idx} sx={{ py: 0.5 }}>
                      <ListItemText
                        primary={alt.domain}
                        secondary={`Confidence: ${(alt.confidence * 100).toFixed(0)}%`}
                      />
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
          </Paper>
        )}
      </Box>
    );
  };

  const renderEntitiesJustification = () => {
    const step1_4 = justification.step_1_4;
    if (!step1_4 || !step1_4.entities) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Entity Extraction Details (Step 1.4)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ mb: 2, color: 'text.secondary' }}>
          Found {step1_4.entities.length} entities with the following details:
        </Typography>
        <List>
          {step1_4.entities.map((entity: any, idx: number) => (
            <Accordion key={idx} sx={{ mb: 1 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, width: '100%' }}>
                  <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                    {entity.name}
                  </Typography>
                  <Chip 
                    label={entity.mention_type || 'unknown'} 
                    size="small" 
                    color={entity.mention_type === 'explicit' ? 'primary' : 'default'}
                  />
                  {entity.confidence !== undefined && (
                    <Chip 
                      label={`${(entity.confidence * 100).toFixed(0)}%`}
                      size="small"
                      variant="outlined"
                    />
                  )}
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <Box>
                  {entity.evidence && (
                    <Box sx={{ mb: 1 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                        Evidence from description:
                      </Typography>
                      <Chip 
                        label={entity.evidence} 
                        size="small" 
                        color="primary" 
                        variant="outlined"
                        sx={{ fontStyle: 'italic' }}
                      />
                    </Box>
                  )}
                  {entity.description && (
                    <Box sx={{ mb: 1 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                        Description:
                      </Typography>
                      <Typography variant="body2" sx={{ color: 'text.secondary' }}>
                        {entity.description}
                      </Typography>
                    </Box>
                  )}
                  {entity.reasoning && (
                    <Box sx={{ mb: 1 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                        Reasoning:
                      </Typography>
                      <Typography variant="body2" sx={{ color: 'text.secondary', fontStyle: 'italic' }}>
                        {entity.reasoning}
                      </Typography>
                    </Box>
                  )}
                  {(entity.cardinality || entity.table_type) && (
                    <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                      {entity.cardinality && (
                        <Chip 
                          label={`Cardinality: ${entity.cardinality}`}
                          size="small"
                          variant="outlined"
                        />
                      )}
                      {entity.table_type && (
                        <Chip 
                          label={`Type: ${entity.table_type}`}
                          size="small"
                          variant="outlined"
                        />
                      )}
                    </Box>
                  )}
                </Box>
              </AccordionDetails>
            </Accordion>
          ))}
        </List>
      </Paper>
    );
  };

  const renderRelationsJustification = () => {
    const step1_9 = justification.step_1_9;
    if (!step1_9) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Relation Extraction Details (Step 1.9)
          </Typography>
        </Box>
        {step1_9.relations && (
          <Typography variant="body2" sx={{ color: 'text.secondary' }}>
            Found {step1_9.relations.length} relations between entities.
          </Typography>
        )}
      </Paper>
    );
  };

  const renderAttributesJustification = () => {
    const step2_2 = justification.step_2_2;
    if (!step2_2) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Attribute Extraction Details (Step 2.2)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Attributes were extracted for all entities based on the description and domain context.
        </Typography>
      </Paper>
    );
  };

  const renderPrimaryKeysJustification = () => {
    const step2_7 = justification.step_2_7;
    if (!step2_7) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Primary Key Identification Details (Step 2.7)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Primary keys were identified for each entity based on uniqueness and stability requirements.
        </Typography>
      </Paper>
    );
  };

  const renderERDiagramJustification = () => {
    const step3_4 = justification.step_3_4;
    if (!step3_4) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            ER Design Compilation (Step 3.4)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          ER design was compiled from entities, relations, attributes, and primary keys.
        </Typography>
      </Paper>
    );
  };

  const renderMultivaluedDerivedJustification = () => {
    const step2_8 = justification.step_2_8;
    const step2_9 = justification.step_2_9;
    if (!step2_8 && !step2_9) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Multivalued/Derived Detection (Steps 2.8 & 2.9)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Multivalued and derived attributes were identified. Derived attributes have DSL formulas for computation.
        </Typography>
      </Paper>
    );
  };

  const renderNullabilityJustification = () => {
    const step2_11 = justification.step_2_11;
    if (!step2_11) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Nullability Constraints (Step 2.11)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Nullability constraints were identified for each attribute based on business requirements and primary key constraints.
        </Typography>
      </Paper>
    );
  };

  const renderDefaultValuesJustification = () => {
    const step2_12 = justification.step_2_12;
    if (!step2_12) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Default Values (Step 2.12)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Default values were identified for attributes that should have automatic initialization.
        </Typography>
      </Paper>
    );
  };

  const renderCheckConstraintsJustification = () => {
    const step2_13 = justification.step_2_13;
    if (!step2_13) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Check Constraints (Step 2.13)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Check constraints were identified to enforce domain-specific validation rules on attribute values.
        </Typography>
      </Paper>
    );
  };

  const renderPhase2FinalJustification = () => {
    const step2_14 = justification.step_2_14;
    const step2_15 = justification.step_2_15;
    const step2_16 = justification.step_2_16;
    if (!step2_14 && !step2_15 && !step2_16) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Phase 2 Final Steps (Steps 2.14, 2.15, 2.16)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Entity cleanup, relation intrinsic attributes extraction, and cross-entity attribute reconciliation completed.
        </Typography>
      </Paper>
    );
  };

  const renderRelationalSchemaJustification = () => {
    const step3_5 = justification.step_3_5;
    if (!step3_5) return null;

    return (
      <Paper elevation={0} sx={{ p: 2, bgcolor: 'grey.50', border: '1px solid', borderColor: 'divider' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
          <Info color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
            Relational Schema Compilation (Step 3.5)
          </Typography>
        </Box>
        <Typography variant="body2" sx={{ color: 'text.secondary' }}>
          Relational schema was compiled from the ER design using standard ER-to-relational mapping rules.
        </Typography>
      </Paper>
    );
  };

  return (
    <Box sx={{ mt: 3 }}>
      <Typography variant="subtitle2" sx={{ mb: 2, fontWeight: 600, color: 'text.secondary' }}>
        Justification
      </Typography>
      {type === 'domain' && renderDomainJustification()}
      {type === 'entities' && renderEntitiesJustification()}
      {type === 'relations' && renderRelationsJustification()}
      {type === 'attributes' && renderAttributesJustification()}
      {type === 'primary_keys' && renderPrimaryKeysJustification()}
      {type === 'multivalued_derived' && renderMultivaluedDerivedJustification()}
      {type === 'nullability' && renderNullabilityJustification()}
      {type === 'default_values' && renderDefaultValuesJustification()}
      {type === 'check_constraints' && renderCheckConstraintsJustification()}
      {type === 'phase2_final' && renderPhase2FinalJustification()}
      {type === 'er_diagram' && renderERDiagramJustification()}
      {type === 'relational_schema' && renderRelationalSchemaJustification()}
    </Box>
  );
};

export default JustificationDisplay;
