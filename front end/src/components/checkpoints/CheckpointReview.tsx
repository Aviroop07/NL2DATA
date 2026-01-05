import React from 'react';
import { Box, Paper } from '@mui/material';
import { useAppStore } from '../../stores/useAppStore';
import DomainCheckpoint from './DomainCheckpoint';
import EntitiesCheckpoint from './EntitiesCheckpoint';
import RelationsCheckpoint from './RelationsCheckpoint';
import AttributesCheckpoint from './AttributesCheckpoint';
import PrimaryKeysCheckpoint from './PrimaryKeysCheckpoint';
import MultivaluedDerivedCheckpoint from './MultivaluedDerivedCheckpoint';
import NullabilityCheckpoint from './NullabilityCheckpoint';
import ERDiagramCheckpoint from './ERDiagramCheckpoint';
import DatatypesCheckpoint from './DatatypesCheckpoint';
import RelationalSchemaCheckpoint from './RelationalSchemaCheckpoint';
import InformationMiningCheckpoint from './InformationMiningCheckpoint';
import FunctionalDependenciesCheckpoint from './FunctionalDependenciesCheckpoint';
import ConstraintsCheckpoint from './ConstraintsCheckpoint';
import GenerationStrategiesCheckpoint from './GenerationStrategiesCheckpoint';
import DomainCheckpointReadOnly from './DomainCheckpointReadOnly';
import EntitiesCheckpointReadOnly from './EntitiesCheckpointReadOnly';
import RelationsCheckpointReadOnly from './RelationsCheckpointReadOnly';
import AttributesCheckpointReadOnly from './AttributesCheckpointReadOnly';
import PrimaryKeysCheckpointReadOnly from './PrimaryKeysCheckpointReadOnly';
import MultivaluedDerivedCheckpointReadOnly from './MultivaluedDerivedCheckpointReadOnly';
import NullabilityCheckpointReadOnly from './NullabilityCheckpointReadOnly';
import ERDiagramCheckpointReadOnly from './ERDiagramCheckpointReadOnly';
import DatatypesCheckpointReadOnly from './DatatypesCheckpointReadOnly';
import RelationalSchemaCheckpointReadOnly from './RelationalSchemaCheckpointReadOnly';
import InformationMiningCheckpointReadOnly from './InformationMiningCheckpointReadOnly';
import FunctionalDependenciesCheckpointReadOnly from './FunctionalDependenciesCheckpointReadOnly';
import ConstraintsCheckpointReadOnly from './ConstraintsCheckpointReadOnly';
import GenerationStrategiesCheckpointReadOnly from './GenerationStrategiesCheckpointReadOnly';

const CheckpointReview: React.FC = () => {
  const { currentCheckpoint, completedCheckpoints } = useAppStore();
  
  // Show nothing if no checkpoint and no completed checkpoints
  if ((!currentCheckpoint || currentCheckpoint === "complete") && completedCheckpoints.length === 0) {
    return null;
  }
  
  return (
    <Box sx={{ mb: 3 }}>
      {/* Show completed checkpoints in read-only mode */}
      {completedCheckpoints.map((completed, index) => (
        <Paper 
          key={`completed-${completed.type}-${index}`}
          elevation={0}
          sx={{ 
            p: 0, 
            mb: 3,
            border: '1px solid',
            borderColor: 'divider',
            overflow: 'hidden',
            opacity: 0.8
          }}
        >
          <Box sx={{ p: 4 }}>
            {completed.type === "domain" && (
              <DomainCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "entities" && (
              <EntitiesCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "relations" && (
              <RelationsCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "attributes" && (
              <AttributesCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "primary_keys" && (
              <PrimaryKeysCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "multivalued_derived" && (
              <MultivaluedDerivedCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "nullability" && (
              <NullabilityCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "er_diagram" && (
              <ERDiagramCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "datatypes" && (
              <DatatypesCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "relational_schema" && (
              <RelationalSchemaCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "information_mining" && (
              <InformationMiningCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "functional_dependencies" && (
              <FunctionalDependenciesCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "constraints" && (
              <ConstraintsCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
            {completed.type === "generation_strategies" && (
              <GenerationStrategiesCheckpointReadOnly 
                data={completed.data} 
                justification={completed.justification} 
              />
            )}
          </Box>
        </Paper>
      ))}
      
      {/* Show current checkpoint in editable mode (only if currentCheckpoint is set) */}
      {currentCheckpoint && currentCheckpoint !== "complete" && (
        <Paper 
          elevation={0}
          sx={{ 
            p: 0, 
            mb: 3,
            border: '1px solid',
            borderColor: 'divider',
            overflow: 'hidden'
          }}
        >
          <Box sx={{ p: 4 }}>
            {currentCheckpoint === "domain" && <DomainCheckpoint />}
            {currentCheckpoint === "entities" && <EntitiesCheckpoint />}
            {currentCheckpoint === "relations" && <RelationsCheckpoint />}
            {currentCheckpoint === "attributes" && <AttributesCheckpoint />}
            {currentCheckpoint === "primary_keys" && <PrimaryKeysCheckpoint />}
            {currentCheckpoint === "multivalued_derived" && <MultivaluedDerivedCheckpoint />}
            {currentCheckpoint === "er_diagram" && <ERDiagramCheckpoint />}
            {currentCheckpoint === "relational_schema" && <RelationalSchemaCheckpoint />}
            {currentCheckpoint === "datatypes" && <DatatypesCheckpoint />}
            {currentCheckpoint === "nullability" && <NullabilityCheckpoint />}
            {currentCheckpoint === "information_mining" && <InformationMiningCheckpoint />}
            {currentCheckpoint === "functional_dependencies" && <FunctionalDependenciesCheckpoint />}
            {currentCheckpoint === "constraints" && <ConstraintsCheckpoint />}
            {currentCheckpoint === "generation_strategies" && <GenerationStrategiesCheckpoint />}
          </Box>
        </Paper>
      )}
    </Box>
  );
};

export default CheckpointReview;
