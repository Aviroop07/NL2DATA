import React from 'react';
import { Box, Button, Paper, Typography } from '@mui/material';
import { useAppStore } from '../../stores/useAppStore';
import ERDiagramForm from './ERDiagramForm';

const ERDiagramEditor: React.FC = () => {
  const {
    erDiagram,
    erEditing,
    hasUnsavedERChanges,
    setEREditing,
    saveERChanges,
    discardERChanges
  } = useAppStore();
  
  const handleEdit = () => {
    setEREditing(true);
  };
  
  const handleSave = async () => {
    await saveERChanges();
  };
  
  const handleDiscard = () => {
    discardERChanges();
  };
  
  return (
    <Box sx={{ p: 4 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h5" sx={{ fontWeight: 600, mb: 0.5 }}>
            ER Diagram
          </Typography>
          <Typography variant="body2" color="text.secondary">
            Visual representation of your database entities and relationships
          </Typography>
        </Box>
        <Box>
          {!erEditing && (
            <Button 
              variant="contained" 
              onClick={handleEdit}
              sx={{ minWidth: 100 }}
            >
              Edit
            </Button>
          )}
          {erEditing && (
            <>
              <Button 
                variant="contained" 
                onClick={handleSave} 
                sx={{ mr: 1, minWidth: 120 }} 
                disabled={!hasUnsavedERChanges}
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
      
      {erDiagram?.imageUrl ? (
        <Paper 
          elevation={0}
          sx={{ 
            p: 2, 
            mb: 3,
            border: '1px solid',
            borderColor: 'divider',
            borderRadius: 2,
            bgcolor: 'grey.50',
            textAlign: 'center'
          }}
        >
          <img
            src={erDiagram.imageUrl}
            alt="ER Diagram"
            style={{ 
              maxWidth: '100%', 
              height: 'auto',
              borderRadius: 8
            }}
          />
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
            ER Diagram will appear here after processing
          </Typography>
        </Paper>
      )}
      
      {erEditing && (
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
          <ERDiagramForm />
        </Paper>
      )}
    </Box>
  );
};

export default ERDiagramEditor;

