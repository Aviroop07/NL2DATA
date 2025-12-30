import React from 'react';
import { Box, Button, Paper, Typography } from '@mui/material';
import { useAppStore } from '../../stores/useAppStore';
import RelationalSchemaForm from './RelationalSchemaForm';

const RelationalSchemaEditor: React.FC = () => {
  const {
    relationalSchema,
    schemaEditing,
    hasUnsavedSchemaChanges,
    schemaEditButtonEnabled,
    setSchemaEditing,
    saveSchemaChanges,
    discardSchemaChanges
  } = useAppStore();
  
  const handleEdit = () => {
    setSchemaEditing(true);
  };
  
  const handleSave = async () => {
    await saveSchemaChanges();
  };
  
  const handleDiscard = () => {
    discardSchemaChanges();
  };
  
  return (
    <Box sx={{ p: 4 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h5" sx={{ fontWeight: 600, mb: 0.5 }}>
            Relational Schema
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Database tables, columns, and relationships
          </Typography>
        </Box>
        <Box>
          {!schemaEditing && (
            <Button 
              variant="contained" 
              onClick={handleEdit}
              disabled={!schemaEditButtonEnabled}
              sx={{ minWidth: 100 }}
            >
              Edit
            </Button>
          )}
          {schemaEditing && (
            <>
              <Button 
                variant="contained" 
                onClick={handleSave} 
                sx={{ mr: 1, minWidth: 120 }} 
                disabled={!hasUnsavedSchemaChanges}
              >
                Save Changes
              </Button>
              <Button 
                variant="outlined" 
                onClick={handleDiscard}
                sx={{ minWidth: 120 }}
              >
                Discard
              </Button>
            </>
          )}
        </Box>
      </Box>
      
      {relationalSchema ? (
        <Paper 
          elevation={0}
          sx={{ 
            p: 3, 
            mb: 3,
            border: '1px solid',
            borderColor: 'divider',
            borderRadius: 2,
            bgcolor: 'grey.50'
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
            <Typography variant="body1" sx={{ fontWeight: 600 }}>
              Schema Summary
            </Typography>
            <Box 
              sx={{ 
                bgcolor: 'primary.main',
                color: 'primary.contrastText',
                px: 2,
                py: 0.5,
                borderRadius: 2,
                fontWeight: 600
              }}
            >
              {relationalSchema.tables?.length || 0} {relationalSchema.tables?.length === 1 ? 'table' : 'tables'}
            </Box>
          </Box>
        </Paper>
      ) : (
        <Paper 
          elevation={0}
          sx={{ 
            p: 6, 
            mb: 3,
            border: '1px dashed',
            borderColor: 'divider',
            borderRadius: 2,
            textAlign: 'center',
            bgcolor: 'grey.50'
          }}
        >
          <Typography variant="body2" color="text.secondary">
            Relational schema will appear here after processing
          </Typography>
        </Paper>
      )}
      
      {schemaEditing && (
        <Paper 
          elevation={0}
          sx={{ 
            p: 3,
            border: '1px solid',
            borderColor: 'primary.main',
            borderRadius: 2,
            bgcolor: 'action.hover'
          }}
        >
          <RelationalSchemaForm />
        </Paper>
      )}
    </Box>
  );
};

export default RelationalSchemaEditor;

