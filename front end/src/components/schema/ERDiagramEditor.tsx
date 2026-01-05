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
    <Box>
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
      ) : erDiagram && (erDiagram.entities?.length > 0 || erDiagram.relations?.length > 0) ? (
        <ERDiagramForm />
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
    </Box>
  );
};

export default ERDiagramEditor;

