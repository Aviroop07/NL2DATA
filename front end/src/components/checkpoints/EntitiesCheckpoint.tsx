import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Alert,
  CircularProgress,
  List,
  ListItem,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions
} from '@mui/material';
import { Save, PlayArrow, Add, Delete, Edit } from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';
import JustificationDisplay from './JustificationDisplay';
import type { Entity } from '../../types/state';

const EntitiesCheckpoint: React.FC = () => {
  const { 
    jobId, 
    entities, 
    checkpointData,
    checkpointJustification,
    saveEntitiesEdit,
    proceedToNextCheckpoint,
    markCheckpointCompleted
  } = useAppStore();
  
  const [editedEntities, setEditedEntities] = useState<Entity[]>([]);
  const [lastCheckpointData, setLastCheckpointData] = useState<any>(null);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [newEntity, setNewEntity] = useState({ name: '', description: '' });
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [hasChanges, setHasChanges] = useState(false);
  
  // Initialize from checkpointData first, then fallback to entities from store
  // Reset when checkpointData changes (new checkpoint loaded)
  useEffect(() => {
    // Check if checkpointData has actually changed
    const checkpointDataKey = JSON.stringify(checkpointData?.entities || []);
    if (checkpointDataKey !== lastCheckpointData) {
      const entitiesFromCheckpoint = checkpointData?.entities || [];
      const entitiesFromStore = entities || [];
      
      // Prioritize checkpointData, but use store entities if checkpointData is empty
      const initialEntities = entitiesFromCheckpoint.length > 0 
        ? entitiesFromCheckpoint.map((e: any) => ({
            name: e.name || '',
            description: e.description || '',
            attributes: []
          }))
        : entitiesFromStore;
      
      setEditedEntities(initialEntities);
      setLastCheckpointData(checkpointDataKey);
    }
  }, [checkpointData, entities, lastCheckpointData]);
  
  useEffect(() => {
    // Check if entities have changed compared to the original checkpoint data
    const originalEntities = checkpointData?.entities || entities || [];
    const originalEntitiesFormatted = originalEntities.map((e: any) => ({
      name: e.name || '',
      description: e.description || '',
      attributes: []
    }));
    const entitiesChanged = JSON.stringify(editedEntities) !== JSON.stringify(originalEntitiesFormatted);
    setHasChanges(entitiesChanged);
  }, [editedEntities, checkpointData, entities]);
  
  const handleSave = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      await saveEntitiesEdit(jobId, editedEntities);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to save entities');
      setSaving(false);
    }
  };
  
  const handleProceed = async () => {
    if (!jobId) return;
    
    setSaving(true);
    setError(null);
    
    try {
      // Save entities to backend first to ensure state is updated
      await saveEntitiesEdit(jobId, editedEntities);
      
      // Mark current checkpoint as completed before proceeding
      const entitiesData = editedEntities.map(e => ({
        name: e.name,
        description: e.description,
        mention_type: "explicit" as const,
        evidence: "",
        confidence: 1.0
      }));
      markCheckpointCompleted("entities", { ...checkpointData, entities: entitiesData });
      await proceedToNextCheckpoint(jobId);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to proceed');
      setSaving(false);
    }
  };
  
  const handleDelete = (index: number) => {
    setEditedEntities(editedEntities.filter((_, i) => i !== index));
  };
  
  const handleEdit = (index: number) => {
    setEditingIndex(index);
  };
  
  const handleSaveEdit = (index: number, name: string, description: string) => {
    const updated = [...editedEntities];
    updated[index] = { name, description, attributes: updated[index].attributes };
    setEditedEntities(updated);
    setEditingIndex(null);
  };
  
  const handleAdd = () => {
    if (newEntity.name.trim()) {
      setEditedEntities([...editedEntities, { 
        name: newEntity.name.trim(), 
        description: newEntity.description.trim() || '',
        attributes: []
      }]);
      setNewEntity({ name: '', description: '' });
      setAddDialogOpen(false);
    }
  };
  
  return (
    <Box>
      <Typography variant="h5" sx={{ fontWeight: 600, mb: 3 }}>
        Entities Review
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
            Found {editedEntities.length} entities
          </Typography>
          <Button
            variant="outlined"
            size="small"
            startIcon={<Add />}
            onClick={() => setAddDialogOpen(true)}
          >
            Add Entity
          </Button>
        </Box>
        
        <List>
          {editedEntities.map((entity, index) => (
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
              {editingIndex === index ? (
                <EntityEditForm
                  entity={entity}
                  onSave={(name, description) => handleSaveEdit(index, name, description)}
                  onCancel={() => setEditingIndex(null)}
                />
              ) : (
                <Box sx={{ width: '100%', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Box sx={{ flex: 1 }}>
                    <Typography variant="subtitle1" sx={{ fontWeight: 600 }}>
                      {entity.name}
                    </Typography>
                    <Typography variant="body2" sx={{ color: 'text.secondary', mt: 0.5 }}>
                      {entity.description || 'No description'}
                    </Typography>
                  </Box>
                  <Box>
                    <IconButton size="small" onClick={() => handleEdit(index)}>
                      <Edit />
                    </IconButton>
                    <IconButton size="small" onClick={() => handleDelete(index)} color="error">
                      <Delete />
                    </IconButton>
                  </Box>
                </Box>
              )}
            </ListItem>
          ))}
        </List>
        
        {checkpointJustification && (
          <Box sx={{ mt: 3 }}>
            <JustificationDisplay justification={checkpointJustification} type="entities" />
          </Box>
        )}
      </Paper>
      
      <Dialog open={addDialogOpen} onClose={() => setAddDialogOpen(false)}>
        <DialogTitle>Add New Entity</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            margin="dense"
            label="Entity Name"
            fullWidth
            variant="outlined"
            value={newEntity.name}
            onChange={(e) => setNewEntity({ ...newEntity, name: e.target.value })}
            sx={{ mb: 2 }}
          />
          <TextField
            margin="dense"
            label="Description"
            fullWidth
            multiline
            rows={3}
            variant="outlined"
            value={newEntity.description}
            onChange={(e) => setNewEntity({ ...newEntity, description: e.target.value })}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setAddDialogOpen(false)}>Cancel</Button>
          <Button onClick={handleAdd} variant="contained" disabled={!newEntity.name.trim()}>
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
          disabled={saving || editedEntities.length === 0}
        >
          Proceed
        </Button>
      </Box>
    </Box>
  );
};

const EntityEditForm: React.FC<{
  entity: Entity;
  onSave: (name: string, description: string) => void;
  onCancel: () => void;
}> = ({ entity, onSave, onCancel }) => {
  const [name, setName] = useState(entity.name);
  const [description, setDescription] = useState(entity.description || '');
  
  return (
    <Box sx={{ width: '100%', display: 'flex', gap: 2, alignItems: 'center' }}>
      <TextField
        size="small"
        value={name}
        onChange={(e) => setName(e.target.value)}
        placeholder="Entity name"
        sx={{ flex: 1 }}
      />
      <TextField
        size="small"
        value={description}
        onChange={(e) => setDescription(e.target.value)}
        placeholder="Description"
        sx={{ flex: 2 }}
      />
      <Button size="small" onClick={() => onSave(name, description)}>
        Save
      </Button>
      <Button size="small" onClick={onCancel}>
        Cancel
      </Button>
    </Box>
  );
};

export default EntitiesCheckpoint;
