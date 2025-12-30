import React from 'react';
import { 
  Box, 
  Typography, 
  Paper, 
  Chip, 
  Divider,
  Tooltip,
  IconButton
} from '@mui/material';
import { 
  AutoAwesome, 
  Domain, 
  TableChart, 
  Key, 
  Link, 
  Settings,
  Timeline
} from '@mui/icons-material';
import { useAppStore } from '../../stores/useAppStore';

const SuggestionsPanel: React.FC = () => {
  const { keywordSuggestions, setNLDescription, processing } = useAppStore();
  
  const handleKeywordClick = (enhancedDescription: string) => {
    // Don't allow clicks when processing
    if (processing) {
      return;
    }
    setNLDescription(enhancedDescription);
  };
  
  const getTypeInfo = (type: string): { icon: React.ReactNode; color: string; label: string } => {
    const typeMap: Record<string, { icon: React.ReactNode; color: string; label: string }> = {
      domain: {
        icon: <Domain fontSize="small" />,
        color: "#1976d2",
        label: "Domain"
      },
      entity: {
        icon: <TableChart fontSize="small" />,
        color: "#9c27b0",
        label: "Entity"
      },
      constraint: {
        icon: <Key fontSize="small" />,
        color: "#d32f2f",
        label: "Constraint"
      },
      attribute: {
        icon: <Settings fontSize="small" />,
        color: "#0288d1",
        label: "Attribute"
      },
      relationship: {
        icon: <Link fontSize="small" />,
        color: "#388e3c",
        label: "Relationship"
      },
      distribution: {
        icon: <Timeline fontSize="small" />,
        color: "#f57c00",
        label: "Distribution"
      }
    };
    
    return typeMap[type] || {
      icon: <AutoAwesome fontSize="small" />,
      color: "#757575",
      label: type
    };
  };
  
  if (keywordSuggestions.length === 0) {
    return (
      <Paper 
        sx={{ 
          p: 3, 
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
          color: 'white'
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <AutoAwesome sx={{ mr: 1 }} />
          <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Suggestions
          </Typography>
        </Box>
        <Divider sx={{ bgcolor: 'rgba(255,255,255,0.3)', mb: 2 }} />
        <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Typography variant="body2" sx={{ opacity: 0.9, textAlign: 'center' }}>
            Start typing to get intelligent suggestions for improving your database description
          </Typography>
        </Box>
      </Paper>
    );
  }
  
  return (
    <Box 
      sx={{ 
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        overflow: 'hidden'
      }}
    >
      <Box sx={{ p: 3, pb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <AutoAwesome sx={{ mr: 1.5, fontSize: 28 }} />
          <Typography variant="h6" sx={{ fontWeight: 700, flex: 1 }}>
            AI Suggestions
          </Typography>
          <Box 
            sx={{ 
              bgcolor: 'rgba(255,255,255,0.25)', 
              px: 2, 
              py: 0.75, 
              borderRadius: 3,
              border: '1px solid rgba(255,255,255,0.3)',
            }}
          >
            <Typography variant="body2" sx={{ fontWeight: 700 }}>
              {keywordSuggestions.length}
            </Typography>
          </Box>
        </Box>
        <Typography variant="body2" sx={{ opacity: 0.9, fontSize: '0.875rem' }}>
          Click any suggestion to enhance your description
        </Typography>
      </Box>
      
      <Divider sx={{ bgcolor: 'rgba(255,255,255,0.2)', mx: 3 }} />
      
      <Box sx={{ flex: 1, overflow: 'auto', p: 3, pt: 2 }}>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
          {keywordSuggestions.map((keyword, index) => {
            const typeInfo = getTypeInfo(keyword.type);
            return (
              <Tooltip 
                key={index}
                title={processing ? 'Processing in progress - suggestions disabled' : `Click to add "${keyword.text}" to your description`}
                arrow
                placement="left"
              >
                <Box
                  onClick={() => handleKeywordClick(keyword.enhanced_nl_description)}
                  sx={{
                    cursor: processing ? 'not-allowed' : 'pointer',
                    bgcolor: processing ? 'rgba(255,255,255,0.08)' : 'rgba(255,255,255,0.12)',
                    border: '1px solid rgba(255,255,255,0.25)',
                    borderRadius: 2,
                    p: 2,
                    opacity: processing ? 0.6 : 1,
                    transition: 'all 0.2s ease',
                    '&:hover': processing ? {} : {
                      bgcolor: 'rgba(255,255,255,0.2)',
                      transform: 'translateX(4px)',
                      boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                      borderColor: 'rgba(255,255,255,0.4)',
                    },
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5 }}>
                    <Box 
                      sx={{ 
                        color: 'white',
                        mt: 0.5,
                        display: 'flex',
                        alignItems: 'center'
                      }}
                    >
                      {typeInfo.icon}
                    </Box>
                    <Box sx={{ flex: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                        <Typography variant="body1" sx={{ fontWeight: 600, fontSize: '0.95rem' }}>
                          {keyword.text}
                        </Typography>
                        <Box
                          sx={{
                            bgcolor: 'rgba(255,255,255,0.25)',
                            px: 1,
                            py: 0.25,
                            borderRadius: 1.5,
                            border: '1px solid rgba(255,255,255,0.3)',
                          }}
                        >
                          <Typography 
                            variant="caption" 
                            sx={{ 
                              fontWeight: 600,
                              fontSize: '0.7rem',
                              textTransform: 'uppercase',
                              letterSpacing: '0.5px'
                            }}
                          >
                            {typeInfo.label}
                          </Typography>
                        </Box>
                      </Box>
                      <Typography 
                        variant="caption" 
                        sx={{ 
                          opacity: 0.85,
                          fontSize: '0.8rem',
                          lineHeight: 1.4,
                          display: 'block',
                          mt: 0.5
                        }}
                      >
                        Click to add this keyword
                      </Typography>
                    </Box>
                  </Box>
                </Box>
              </Tooltip>
            );
          })}
        </Box>
      </Box>
    </Box>
  );
};

export default SuggestionsPanel;
