import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Alert,
  CircularProgress,
  TextField,
  List,
  ListItem,
  ListItemText,
  IconButton,
  Chip,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Divider
} from '@mui/material';
import { Save, PlayArrow, ExpandMore, Delete, Add, CheckCircle, Error as ErrorIcon } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

interface InformationNeed {
  description: string;
  sql_query: string;
  entities_involved?: string[];
  is_valid?: boolean;
  validation_error?: string;
}

const InformationMiningCheckpoint: React.FC = () => {
  const { 
    jobId, 
    checkpointData,
    checkpointJustification,
    saveInformationMiningEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [informationNeeds, setInformationNeeds] = useState<InformationNeed[]>(
    checkpointData?.information_needs || []
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [validatingIndex, setValidatingIndex] = useState<number | null>(null);
  
  useEffect(() => {
    if (checkpointData?.information_needs) {
      setInformationNeeds(checkpointData.information_needs);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    const needsChanged = JSON.stringify(informationNeeds) !== JSON.stringify(checkpointData?.information_needs || []);
    setHasChanges(needsChanged);
  }, [informationNeeds, checkpointData]);
  
  const validateSQL = async (sqlQuery: string, index: number): Promise<boolean> => {
    if (!sqlQuery.trim()) {
      return false;
    }
    
    setValidatingIndex(index);
    try {
      // SQL validation will be done on backend when saving
      // For now, just check basic syntax
      const trimmed = sqlQuery.trim().toUpperCase();
      if (!trimmed.startsWith('SELECT')) {
        setInformationNeeds(prev => {
          const updated = [...prev];
          updated[index] = {
            ...updated[index],
            is_valid: false,
            validation_error: 'SQL query must start with SELECT'
          };
          return updated;
        });
        setValidatingIndex(null);
        return false;
      }
      
      // Mark as valid for now (backend will do full validation)
      setInformationNeeds(prev => {
        const updated = [...prev];
        updated[index] = {
          ...updated[index],
          is_valid: true,
          validation_error: undefined
        };
        return updated;
      });
      setValidatingIndex(null);
      return true;
    } catch (err) {
      setInformationNeeds(prev => {
        const updated = [...prev];
        updated[index] = {
          ...updated[index],
          is_valid: false,
          validation_error: 'Invalid SQL syntax'
        };
        return updated;
      });
      setValidatingIndex(null);
      return false;
    }
  };
  
  const handleAddNew = () => {
    setInformationNeeds(prev => [...prev, {
      description: '',
      sql_query: '',
      entities_involved: [],
      is_valid: undefined
    }]);
  };
  
  const handleDelete = (index: number) => {
    setInformationNeeds(prev => prev.filter((_, i) => i !== index));
  };
  
  const handleChange = (index: number, field: keyof InformationNeed, value: any) => {
    setInformationNeeds(prev => {
      const updated = [...prev];
      updated[index] = {
        ...updated[index],
        [field]: value,
        is_valid: undefined, // Reset validation when changed
        validation_error: undefined
      };
      return updated;
    });
  };
  
  const handleSave = async () => {
    if (!jobId) return;
    
    // Validate all SQL queries before saving
    const needsToSave = informationNeeds.filter(need => need.description.trim() && need.sql_query.trim());
    
    if (needsToSave.length === 0) {
      setError('At least one information need with description and SQL query is required');
      return;
    }
    
    setSaving(true);
    setError(null);
    
    try {
      await saveInformationMiningEdit(jobId, needsToSave);
      setSaving(false);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save information mining data');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Validate and save first
      const needsToSave = informationNeeds.filter(need => need.description.trim() && need.sql_query.trim());
      
      if (needsToSave.length === 0) {
        setError('At least one information need with description and SQL query is required');
        setSaving(false);
        return;
      }
      
      await saveInformationMiningEdit(jobId, needsToSave);
      
      markCheckpointCompleted("information_mining", { ...checkpointData, information_needs: needsToSave });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Information Mining Checkpoint
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
        Review and edit information needs with their SQL DML queries. You can add new information needs or modify existing ones.
        All SQL queries will be validated against the relational schema before saving.
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
            Information Needs ({informationNeeds.length})
          </Typography>
          <Button
            variant="outlined"
            startIcon={<Add />}
            onClick={handleAddNew}
            size="small"
          >
            Add New
          </Button>
        </Box>
        
        {informationNeeds.length === 0 ? (
          <Alert severity="info">
            No information needs found. Click "Add New" to add an information need with SQL query.
          </Alert>
        ) : (
          <List>
            {informationNeeds.map((need, index) => (
              <React.Fragment key={index}>
                <ListItem
                  sx={{
                    flexDirection: 'column',
                    alignItems: 'stretch',
                    bgcolor: index % 2 === 0 ? 'background.default' : 'background.paper',
                    borderRadius: 1,
                    mb: 1,
                    p: 2
                  }}
                >
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 2, width: '100%' }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
                      Information Need #{index + 1}
                    </Typography>
                    <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                      {need.is_valid === true && (
                        <Chip icon={<CheckCircle />} label="Valid" color="success" size="small" />
                      )}
                      {need.is_valid === false && (
                        <Chip icon={<ErrorIcon />} label="Invalid" color="error" size="small" />
                      )}
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => handleDelete(index)}
                        disabled={saving}
                      >
                        <Delete />
                      </IconButton>
                    </Box>
                  </Box>
                  
                  <TextField
                    fullWidth
                    label="Description"
                    value={need.description}
                    onChange={(e) => handleChange(index, 'description', e.target.value)}
                    multiline
                    rows={2}
                    sx={{ mb: 2 }}
                    disabled={saving}
                    required
                  />
                  
                  <TextField
                    fullWidth
                    label="SQL DML Query"
                    value={need.sql_query}
                    onChange={(e) => handleChange(index, 'sql_query', e.target.value)}
                    multiline
                    rows={4}
                    sx={{ mb: 1 }}
                    disabled={saving}
                    required
                    placeholder="SELECT ... FROM ... WHERE ..."
                    helperText={need.validation_error || "SQL SELECT statement to retrieve this information"}
                    error={need.is_valid === false}
                  />
                  
                  {need.validation_error && (
                    <Alert severity="error" sx={{ mt: 1 }}>
                      {need.validation_error}
                    </Alert>
                  )}
                  
                  {need.entities_involved && need.entities_involved.length > 0 && (
                    <Box sx={{ mt: 1 }}>
                      <Typography variant="caption" color="text.secondary">
                        Entities involved: {need.entities_involved.join(', ')}
                      </Typography>
                    </Box>
                  )}
                  
                  <Button
                    variant="outlined"
                    size="small"
                    onClick={() => validateSQL(need.sql_query, index)}
                    disabled={saving || !need.sql_query.trim() || validatingIndex === index}
                    sx={{ mt: 1, alignSelf: 'flex-start' }}
                  >
                    {validatingIndex === index ? <CircularProgress size={16} /> : 'Validate SQL'}
                  </Button>
                </ListItem>
                {index < informationNeeds.length - 1 && <Divider />}
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

export default InformationMiningCheckpoint;
