import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Alert,
  CircularProgress,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  List,
  ListItem,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions
} from '@mui/material';
import { Save, PlayArrow, Add, Delete, Edit, ExpandMore } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';

const AttributesCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities,
    checkpointData,
    checkpointJustification,
    saveAttributesEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedAttributes, setEditedAttributes] = useState<Record<string, any[]>>(
    checkpointData?.attributes || {}
  );
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingEntity, setEditingEntity] = useState<string | null>(null);
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [newAttribute, setNewAttribute] = useState({ name: '', description: '', data_type: '' });
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [selectedEntity, setSelectedEntity] = useState<string>('');
  const [hasChanges, setHasChanges] = useState(false);
  
  useEffect(() => {
    if (checkpointData?.attributes) {
      setEditedAttributes(checkpointData.attributes);
    }
  }, [checkpointData]);
  
  useEffect(() => {
    // Check if attributes have changed
    const attributesChanged = JSON.stringify(editedAttributes) !== JSON.stringify(checkpointData?.attributes || {});
    setHasChanges(attributesChanged);
  }, [editedAttributes, checkpointData]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveAttributesEdit(jobId, editedAttributes);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save attributes');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save attributes to backend first to ensure state is updated
      await saveAttributesEdit(jobId, editedAttributes);
      
      // Mark current checkpoint as completed before proceeding
      markCheckpointCompleted("attributes", { ...checkpointData, attributes: editedAttributes });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const handleDelete = (entityName: string, index: number) => {
    const updated = { ...editedAttributes };
    updated[entityName] = updated[entityName].filter((_, i) => i !== index);
    setEditedAttributes(updated);
  };
  
  const handleEdit = (entityName: string, index: number) => {
    setEditingEntity(entityName);
    setEditingIndex(index);
  };
  
  const handleSaveEdit = (entityName: string, index: number, attribute: any) => {
    const updated = { ...editedAttributes };
    updated[entityName][index] = attribute;
    setEditedAttributes(updated);
    setEditingEntity(null);
    setEditingIndex(null);
  };
  
  const handleAdd = () => {
    if (selectedEntity && newAttribute.name.trim()) {
      const updated = { ...editedAttributes };
      if (!updated[selectedEntity]) {
        updated[selectedEntity] = [];
      }
      updated[selectedEntity].push(newAttribute);
      setEditedAttributes(updated);
      setNewAttribute({ name: '', description: '', data_type: '' });
      setSelectedEntity('');
      setAddDialogOpen(false);
    }
  };
  
  const entityNames = entities?.map(e => e.name) || [];
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Attributes Review
      </Typography>
      
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}
      
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
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="subtitle2" sx={{ color: 'text.secondary' }}>
            Attributes by Entity
          </Typography>
          <Button
            variant="outlined"
            size="small"
            startIcon={<Add />}
            onClick={() => setAddDialogOpen(true)}
            disabled={entityNames.length === 0}
          >
            Add Attribute
          </Button>
        </Box>
        
        {entityNames.map((entityName) => {
          const attributes = editedAttributes[entityName] || [];
          return (
            <Accordion key={entityName} sx={{ mb: 1 }}>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                  {entityName} ({attributes.length} attributes)
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                <List>
                  {attributes.map((attr, index) => (
                    <ListItem
                      key={index}
                      sx={{
                        border: '1px solid',
                        borderColor: 'divider',
                        borderRadius: 1,
                        mb: 1,
                        bgcolor: 'background.paper'
                      }}
                    >
                      {editingEntity === entityName && editingIndex === index ? (
                        <AttributeEditForm
                          attribute={attr}
                          onSave={(att) => handleSaveEdit(entityName, index, att)}
                          onCancel={() => {
                            setEditingEntity(null);
                            setEditingIndex(null);
                          }}
                        />
                      ) : (
                        <Box sx={{ width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                          <Box sx={{ flex: 1 }}>
                            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                              {attr.name}
                            </Typography>
                            <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.5 }}>
                              {attr.data_type && `Type: ${attr.data_type}`} {attr.description ? `- ${attr.description}` : ''}
                            </Typography>
                          </Box>
                          <Box>
                            <IconButton size="small" onClick={() => handleEdit(entityName, index)}>
                              <Edit />
                            </IconButton>
                            <IconButton size="small" onClick={() => handleDelete(entityName, index)} color="error">
                              <Delete />
                            </IconButton>
                          </Box>
                        </Box>
                      )}
                    </ListItem>
                  ))}
                  {attributes.length === 0 && (
                    <Typography variant="body2" sx={{ color: 'text.secondary', p: 2 }}>
                      No attributes for this entity
                    </Typography>
                  )}
                </List>
              </AccordionDetails>
            </Accordion>
          );
        })}
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="attributes" />
          </Box>
        )}
      </Paper>
      
      <Dialog open={addDialogOpen} onClose={() => setAddDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Add New Attribute</DialogTitle>
        <DialogContent>
          <TextField
            select
            fullWidth
            label="Entity"
            value={selectedEntity}
            onChange={(e) => setSelectedEntity(e.target.value)}
            sx={{ mb: 2 }}
            SelectProps={{
              native: true
            }}
          >
            <option value="">Select entity</option>
            {entityNames.map((name) => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </TextField>
          <TextField
            autoFocus
            margin="dense"
            label="Attribute Name"
            fullWidth
            variant="outlined"
            value={newAttribute.name}
            onChange={(e) => setNewAttribute({ ...newAttribute, name: e.target.value })}
            sx={{ mb: 2 }}
          />
          <TextField
            margin="dense"
            label="Data Type (optional)"
            fullWidth
            variant="outlined"
            value={newAttribute.data_type}
            onChange={(e) => setNewAttribute({ ...newAttribute, data_type: e.target.value })}
            sx={{ mb: 2 }}
          />
          <TextField
            margin="dense"
            label="Description (optional)"
            fullWidth
            multiline
            rows={3}
            variant="outlined"
            value={newAttribute.description}
            onChange={(e) => setNewAttribute({ ...newAttribute, description: e.target.value })}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddDialogOpen(false)}>Cancel</Button>
          <Button 
            onClick={handleAdd} 
            variant="contained" 
            disabled={!selectedEntity || !newAttribute.name.trim()}
          >
            Add
          </Button>
        </DialogActions>
      </Dialog>
      
      <Box sx={{ display: 'flex', gap: 2, justifyContent: 'flex-end' }}>
        <Button
          variant="outlined"
          startIcon={saving ? <CircularProgress size={16} /> : <Save />}
          onClick={handleSave}
          disabled={saving || !hasChanges}
        >
          Save Changes
        </Button>
        <Button
          variant="contained"
          startIcon={saving ? <CircularProgress size={16} /> : <PlayArrow />}
          onClick={handleProceed}
          disabled={saving || hasChanges}
        >
          Complete
        </Button>
      </Box>
    </Box>
  );
};

const AttributeEditForm: React.FC<{
  attribute: any;
  onSave: (attribute: any) => void;
  onCancel: () => void;
}> = ({ attribute, onSave, onCancel }) => {
  const [name, setName] = useState(attribute.name || '');
  const [description, setDescription] = useState(attribute.description || '');
  const [dataType, setDataType] = useState(attribute.data_type || '');
  
  return (
    <Box sx={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 2 }}>
      <TextField
        size="small"
        value={name}
        onChange={(e) => setName(e.target.value)}
        placeholder="Attribute name"
        fullWidth
      />
      <TextField
        size="small"
        value={dataType}
        onChange={(e) => setDataType(e.target.value)}
        placeholder="Data type (optional)"
        fullWidth
      />
      <TextField
        size="small"
        value={description}
        onChange={(e) => setDescription(e.target.value)}
        placeholder="Description"
        fullWidth
        multiline
        rows={2}
      />
      <Box sx={{ display: 'flex', gap: 1, justifyContent: 'flex-end' }}>
        <Button size="small" onClick={() => onSave({ name, description, data_type: dataType })}>
          Save
        </Button>
        <Button size="small" onClick={onCancel}>
          Cancel
        </Button>
      </Box>
    </Box>
  );
};

export default AttributesCheckpoint;
