import React, { useState, useEffect, useMemo } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  CircularProgress,
  List,
  ListItem,
  IconButton,
  Chip,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  OutlinedInput,
  Divider
} from '@mui/material';
import { Save, PlayArrow, Delete, Add, Error as ErrorIcon } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

interface FunctionalDependency {
  lhs: string[];  // List of attributes (Entity.attribute format)
  rhs: string[];  // List of attributes (Entity.attribute format)
  reasoning?: string;
}

const FunctionalDependenciesCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    saveFunctionalDependenciesEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const primaryKeys = checkpointData?.primary_keys || {};
  
  const [functionalDependencies, setFunctionalDependencies] = useState<FunctionalDependency[]>(
    checkpointData?.functional_dependencies || []
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [validationErrors, setValidationErrors] = useState<Record<number, string>>({});
  
  // Extract all attributes from relational schema for dropdowns
  const allAttributes = useMemo(() => {
    const attrs: string[] = [];
    const relationalSchema = checkpointData?.relational_schema || {};
    const tables = relationalSchema.tables || [];
    
    tables.forEach((table: any) => {
      const tableName = table.name || '';
      const columns = table.columns || [];
      columns.forEach((col: any) => {
        const colName = col.name || '';
        if (tableName && colName) {
          attrs.push(`${tableName}.${colName}`);
        }
      });
    });
    
    return attrs.sort();
  }, [checkpointData]);
  
  // Group attributes by entity/table for better organization
  const attributesByEntity = useMemo(() => {
    const grouped: Record<string, string[]> = {};
    allAttributes.forEach(attr => {
      if (attr.includes('.')) {
        const [entity, attribute] = attr.split('.', 2);
        if (!grouped[entity]) {
          grouped[entity] = [];
        }
        grouped[entity].push(attribute);
      }
    });
    return grouped;
  }, [allAttributes]);
  
  useEffect(() => {
    if (checkpointData?.functional_dependencies) {
      setFunctionalDependencies(checkpointData.functional_dependencies);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    const fdsChanged = JSON.stringify(functionalDependencies) !== JSON.stringify(checkpointData?.functional_dependencies || []);
    setHasChanges(fdsChanged);
  }, [functionalDependencies, checkpointData]);
  
  const validateFD = (fd: FunctionalDependency, index: number): string | null => {
    // Check 1: LHS and RHS must not be empty
    if (!fd.lhs || fd.lhs.length === 0) {
      return 'LHS (left-hand side) cannot be empty';
    }
    if (!fd.rhs || fd.rhs.length === 0) {
      return 'RHS (right-hand side) cannot be empty';
    }
    
    const lhsSet = new Set(fd.lhs);
    const rhsSet = new Set(fd.rhs);
    
    // Check 2: No common attributes in lhs and rhs
    const common = [...lhsSet].filter(attr => rhsSet.has(attr));
    if (common.length > 0) {
      return `Common attributes in LHS and RHS: ${common.join(', ')}`;
    }
    
    // Check 3: No duplicate attributes in lhs
    if (fd.lhs.length !== lhsSet.size) {
      return 'Duplicate attributes in LHS';
    }
    
    // Check 4: No duplicate attributes in rhs
    if (fd.rhs.length !== rhsSet.size) {
      return 'Duplicate attributes in RHS';
    }
    
    // Check 5: All attributes must exist in schema
    const allAttrs = [...fd.lhs, ...fd.rhs];
    const invalidAttrs = allAttrs.filter(attr => !allAttributes.includes(attr));
    if (invalidAttrs.length > 0) {
      return `Invalid attributes: ${invalidAttrs.join(', ')}`;
    }
    
    // Check 6: All attributes must be from same entity
    const entities = new Set<string>();
    allAttrs.forEach(attr => {
      if (attr.includes('.')) {
        entities.add(attr.split('.', 2)[0]);
      }
    });
    if (entities.size > 1) {
      return `Attributes span multiple entities: ${Array.from(entities).join(', ')}. All attributes must be from the same entity.`;
    }
    
    // Check 7: No PK attributes in lhs (trivial FDs - PK determines all attributes)
    const lhsPkIntersection: string[] = [];
    for (const attr of fd.lhs) {
      if (attr.includes('.')) {
        const [entityName, attrName] = attr.split('.', 2);
        const pkList = primaryKeys[entityName] || [];
        if (pkList.includes(attrName)) {
          lhsPkIntersection.push(attr);
        }
      }
    }
    if (lhsPkIntersection.length > 0) {
      return `Primary key attributes in LHS (trivial - PK determines all attributes): ${lhsPkIntersection.join(', ')}`;
    }
    
    // Check 8: LHS should not be a superset of any PK (also trivial)
    for (const [entityName, pkList] of Object.entries(primaryKeys)) {
      if (pkList && pkList.length > 0) {
        const pkSet = new Set(pkList.map(pk => `${entityName}.${pk}`));
        if ([...pkSet].every(pkAttr => lhsSet.has(pkAttr))) {
          return `LHS contains a complete primary key (trivial): ${entityName} PK [${pkList.join(', ')}]`;
        }
      }
    }
    
    // Check 9: LHS should not be a subset of RHS (trivial)
    if (lhsSet.size > 0 && [...lhsSet].every(attr => rhsSet.has(attr))) {
      return 'LHS is a subset of RHS (trivial functional dependency)';
    }
    
    // Check 10: Check for duplicate FD (compare with other FDs)
    for (let i = 0; i < functionalDependencies.length; i++) {
      if (i === index) continue;
      const otherFD = functionalDependencies[i];
      const otherLhsSet = new Set(otherFD.lhs.sort());
      const otherRhsSet = new Set(otherFD.rhs.sort());
      const thisLhsSet = new Set(fd.lhs.sort());
      const thisRhsSet = new Set(fd.rhs.sort());
      
      if (thisLhsSet.size === otherLhsSet.size && 
          thisRhsSet.size === otherRhsSet.size &&
          [...thisLhsSet].every(attr => otherLhsSet.has(attr)) &&
          [...thisRhsSet].every(attr => otherRhsSet.has(attr))) {
        return 'Duplicate functional dependency';
      }
    }
    
    return null;
  };
  
  const handleAddNew = () => {
    setFunctionalDependencies(prev => [...prev, {
      lhs: [],
      rhs: [],
      reasoning: ''
    }]);
  };
  
  const handleDelete = (index: number) => {
    setFunctionalDependencies(prev => prev.filter((_, i) => i !== index));
    setValidationErrors(prev => {
      const updated = { ...prev };
      delete updated[index];
      // Renumber errors for indices after deleted one
      const newErrors: Record<number, string> = {};
      Object.keys(updated).forEach(key => {
        const idx = parseInt(key);
        if (idx < index) {
          newErrors[idx] = updated[idx];
        } else if (idx > index) {
          newErrors[idx - 1] = updated[idx];
        }
      });
      return newErrors;
    });
  };
  
  const handleLHSChange = (index: number, selectedAttributes: string[]) => {
    setFunctionalDependencies(prev => {
      const updated = [...prev];
      updated[index] = {
        ...updated[index],
        lhs: selectedAttributes
      };
      return updated;
    });
    
    // Validate immediately
    const updatedFD = { ...functionalDependencies[index], lhs: selectedAttributes };
    const error = validateFD(updatedFD, index);
    if (error) {
      setValidationErrors(prev => ({ ...prev, [index]: error }));
    } else {
      setValidationErrors(prev => {
        const updated = { ...prev };
        delete updated[index];
        return updated;
      });
    }
  };
  
  const handleRHSChange = (index: number, selectedAttributes: string[]) => {
    setFunctionalDependencies(prev => {
      const updated = [...prev];
      updated[index] = {
        ...updated[index],
        rhs: selectedAttributes
      };
      return updated;
    });
    
    // Validate immediately
    const updatedFD = { ...functionalDependencies[index], rhs: selectedAttributes };
    const error = validateFD(updatedFD, index);
    if (error) {
      setValidationErrors(prev => ({ ...prev, [index]: error }));
    } else {
      setValidationErrors(prev => {
        const updated = { ...prev };
        delete updated[index];
        return updated;
      });
    }
  };
  
  const handleSave = async () => {
    if (!jobId) return;
    
    // Validate all FDs before saving
    const errors: Record<number, string> = {};
    functionalDependencies.forEach((fd, index) => {
      const error = validateFD(fd, index);
      if (error) {
        errors[index] = error;
      }
    });
    
    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      setError('Please fix validation errors before saving');
      return;
    }
    
    const validFDs = functionalDependencies.filter((fd, index) => !errors[index]);
    
    if (validFDs.length === 0) {
      setError('At least one valid functional dependency is required');
      return;
    }
    
    setSaving(true);
    setError(null);
    
    try {
      await saveFunctionalDependenciesEdit(jobId, validFDs);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save functional dependencies');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Validate all FDs before saving
      const errors: Record<number, string> = {};
      functionalDependencies.forEach((fd, index) => {
        const error = validateFD(fd, index);
        if (error) {
          errors[index] = error;
        }
      });
      
      if (Object.keys(errors).length > 0) {
        setValidationErrors(errors);
        setError('Please fix validation errors before proceeding');
        setSaving(false);
        return;
      }
      
      const validFDs = functionalDependencies.filter((fd, index) => !errors[index]);
      
      await saveFunctionalDependenciesEdit(jobId, validFDs);
      
      markCheckpointCompleted("functional_dependencies", { ...checkpointData, functional_dependencies: validFDs });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Functional Dependencies Checkpoint
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Review and edit functional dependencies. Each dependency has a left-hand side (LHS) that determines the right-hand side (RHS).
        All attributes must be from the same entity. Primary keys in LHS are not allowed (trivial FDs).
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}
      
      {checkpointJustification && (
        <Box sx={{ mb: 3 }}>
          <JustificationDisplay justification={checkpointJustification} />
        </Box>
      )}
      
      <Paper sx={{ p: 2, mb: 2 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h6">
            Functional Dependencies ({functionalDependencies.length})
          </Typography>
          <Button
            variant="outlined"
            startIcon={<Add />}
            onClick={handleAddNew}
            size="small"
            disabled={saving}
          >
            Add New FD
          </Button>
        </Box>
        
        {functionalDependencies.length === 0 ? (
          <Alert severity="info">
            No functional dependencies found. Click "Add New FD" to add a functional dependency.
          </Alert>
        ) : (
          <List>
            {functionalDependencies.map((fd, index) => (
              <React.Fragment key={index}>
                <ListItem
                  sx={{
                    flexDirection: 'column',
                    alignItems: 'stretch',
                    bgcolor: index % 2 === 0 ? 'background.default' : 'background.paper',
                    borderRadius: 1,
                    mb: 1,
                    p: 2,
                    border: validationErrors[index] ? '1px solid' : 'none',
                    borderColor: validationErrors[index] ? 'error.main' : 'transparent'
                  }}
                >
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2, width: '100%' }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
                      Functional Dependency #{index + 1}
                    </Typography>
                    <IconButton
                      size="small"
                      color="error"
                      onClick={() => handleDelete(index)}
                      disabled={saving}
                    >
                      <Delete />
                    </IconButton>
                  </Box>
                  
                  {validationErrors[index] && (
                    <Alert severity="error" sx={{ mb: 2 }}>
                      {validationErrors[index]}
                    </Alert>
                  )}
                  
                  <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
                    <FormControl sx={{ minWidth: 200, flex: 1 }}>
                      <InputLabel>Left-Hand Side (LHS)</InputLabel>
                      <Select
                        multiple
                        value={fd.lhs}
                        onChange={(e) => handleLHSChange(index, e.target.value as string[])}
                        input={<OutlinedInput label="Left-Hand Side (LHS)" />}
                        disabled={saving}
                        renderValue={(selected) => (
                          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                            {selected.map((value) => (
                              <Chip key={value} label={value} size="small" />
                            ))}
                          </Box>
                        )}
                      >
                        {allAttributes.map((attr) => (
                          <MenuItem key={attr} value={attr}>
                            {attr}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                    
                    <Typography variant="h6" sx={{ alignSelf: 'center', mx: 1 }}>
                      →
                    </Typography>
                    
                    <FormControl sx={{ minWidth: 200, flex: 1 }}>
                      <InputLabel>Right-Hand Side (RHS)</InputLabel>
                      <Select
                        multiple
                        value={fd.rhs}
                        onChange={(e) => handleRHSChange(index, e.target.value as string[])}
                        input={<OutlinedInput label="Right-Hand Side (RHS)" />}
                        disabled={saving}
                        renderValue={(selected) => (
                          <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                            {selected.map((value) => (
                              <Chip key={value} label={value} size="small" />
                            ))}
                          </Box>
                        )}
                      >
                        {allAttributes.map((attr) => (
                          <MenuItem key={attr} value={attr}>
                            {attr}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                  </Box>
                  
                  {fd.reasoning && (
                    <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                      Reasoning: {fd.reasoning}
                    </Typography>
                  )}
                </ListItem>
                {index < functionalDependencies.length - 1 && <Divider />}
              </React.Fragment>
            ))}
          </List>
        )}
      </Paper>
      
      <Box sx={{ display: 'flex', gap: 2, justifyContent: 'flex-end', mt: 3 }}>
        <Button
          variant="outlined"
          onClick={handleSave}
          disabled={saving || !hasChanges}
          startIcon={saving ? <CircularProgress size={16} /> : <Save />}
        >
          Save
        </Button>
        <Button
          variant="contained"
          onClick={handleProceed}
          disabled={saving}
          startIcon={saving ? <CircularProgress size={16} /> : <PlayArrow />}
        >
          Save & Proceed
        </Button>
      </Box>
    </Box>
  );
};

export default FunctionalDependenciesCheckpoint;
