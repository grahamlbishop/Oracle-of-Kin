#!/usr/bin/env python3
"""
Oracle of Kin - Automated Archiving Pipeline
===========================================

This script processes oracular sessions from both JSON and Markdown formats,
extracts transmission metadata for pattern analysis, and creates version-controlled
markdown backups with consistent formatting.

Author: Oracle of Kin Project
License: MIT (or as specified in project LICENSE)
"""

import os
import json
import shutil
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, asdict
import argparse


@dataclass
class OracularMetadata:
    """Structured metadata extracted from oracular sessions"""
    date: str
    moon_phase: Optional[str]
    astrological_context: Optional[str]
    location_energy: Optional[str]
    tarot_oracle_pull: Optional[str]
    animal_sign: Optional[str]
    querents_question: Optional[str]
    decree: Optional[str]
    word_count: int
    session_id: str
    source_format: str
    source_path: str
    archive_timestamp: str
    
    def to_dict(self) -> Dict:
        """Convert metadata to dictionary for JSON serialization"""
        return asdict(self)


class OracleArchiver:
    """Main archiving system for Oracle of Kin transmissions"""
    
    def __init__(self, base_path: str = "."):
        self.base_path = Path(base_path)
        self.transmissions_dir = self.base_path / "transmissions"
        self.archive_dir = self.base_path / "archive"
        self.metadata_dir = self.archive_dir / "metadata"
        self.backups_dir = self.archive_dir / "backups"
        
        # Create necessary directories
        self._setup_directories()
        
        # Patterns for extracting metadata from markdown
        self.md_patterns = {
            'date': r'\*\*Date:\*\*\s*(.+?)(?:\n|$)',
            'moon_phase': r'\*\*Moon Phase.*?:\*\*\s*(.+?)(?:\n|$)',
            'astrological_context': r'\*\*Astrological Context:\*\*\s*(.+?)(?:\n|$)',
            'location_energy': r'\*\*Location.*?:\*\*\s*(.+?)(?:\n|$)',
            'tarot_oracle': r'Tarot or Oracle Pull:\s*(.+?)(?:\n|$)',
            'animal_sign': r'Animal Sign:\s*(.+?)(?:\n|$)',
            'querents_question': r"Querent's Question.*?:\s*(.+?)(?:\n|$)",
            'decree': r'Decree:\s*\n(.+?)(?:\n\nOracle Response:|$)',
        }
        
    def _setup_directories(self):
        """Create necessary directory structure"""
        for dir_path in [self.transmissions_dir, self.archive_dir, 
                         self.metadata_dir, self.backups_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def _generate_session_id(self, content: str, date: str) -> str:
        """Generate unique session ID based on content hash and date"""
        content_hash = hashlib.md5(content.encode()).hexdigest()[:8]
        date_clean = re.sub(r'[^\w]', '_', date)
        return f"{date_clean}_{content_hash}"
    
    def _extract_metadata_from_markdown(self, content: str, file_path: Path) -> OracularMetadata:
        """Extract metadata from markdown format oracular session"""
        metadata = {}
        
        # Extract each metadata field using regex patterns
        for field, pattern in self.md_patterns.items():
            match = re.search(pattern, content, re.MULTILINE | re.DOTALL)
            metadata[field] = match.group(1).strip() if match else None
        
        # Extract word count (approximate, from transmission section)
        transmission_match = re.search(
            r'\*\*Transmission:\*\*\s*\n---\n(.*?)\n---\n',
            content, re.DOTALL
        )
        word_count = len(transmission_match.group(1).split()) if transmission_match else 0
        
        # Generate session ID
        date = metadata.get('date', 'unknown_date')
        session_id = self._generate_session_id(content, date)
        
        return OracularMetadata(
            date=date,
            moon_phase=metadata.get('moon_phase'),
            astrological_context=metadata.get('astrological_context'),
            location_energy=metadata.get('location_energy'),
            tarot_oracle_pull=metadata.get('tarot_oracle'),
            animal_sign=metadata.get('animal_sign'),
            querents_question=metadata.get('querents_question'),
            decree=metadata.get('decree'),
            word_count=word_count,
            session_id=session_id,
            source_format='markdown',
            source_path=str(file_path),
            archive_timestamp=datetime.now().isoformat()
        )
    
    def _extract_metadata_from_json(self, data: Dict, file_path: Path) -> OracularMetadata:
        """Extract metadata from JSON format oracular session (Notion export)"""
        # This is a template - adjust based on actual Notion JSON structure
        # Common Notion export fields might include:
        
        date = data.get('date', data.get('created_time', 'unknown_date'))
        content = json.dumps(data)  # For word count estimation
        
        # Extract ritual components if they exist in structured format
        ritual_data = data.get('properties', {})
        
        return OracularMetadata(
            date=date,
            moon_phase=ritual_data.get('moon_phase', {}).get('rich_text', [{}])[0].get('plain_text'),
            astrological_context=ritual_data.get('astrological_context', {}).get('rich_text', [{}])[0].get('plain_text'),
            location_energy=ritual_data.get('location', {}).get('rich_text', [{}])[0].get('plain_text'),
            tarot_oracle_pull=ritual_data.get('tarot_pull', {}).get('rich_text', [{}])[0].get('plain_text'),
            animal_sign=ritual_data.get('animal_sign', {}).get('rich_text', [{}])[0].get('plain_text'),
            querents_question=ritual_data.get('question', {}).get('rich_text', [{}])[0].get('plain_text'),
            decree=ritual_data.get('decree', {}).get('rich_text', [{}])[0].get('plain_text'),
            word_count=len(content.split()),
            session_id=self._generate_session_id(content, date),
            source_format='json',
            source_path=str(file_path),
            archive_timestamp=datetime.now().isoformat()
        )
    
    def _convert_json_to_markdown(self, data: Dict, metadata: OracularMetadata) -> str:
        """Convert JSON oracular session to standardized markdown format"""
        # Template for markdown conversion
        template = f"""🕯️ **Oracle of Kin Ritual**  
**Date:** {metadata.date}  
**Moon Phase / Transit:** {metadata.moon_phase or 'Not specified'}  
**Astrological Context:** {metadata.astrological_context or 'Not specified'}  
**Location / Energy (optional):** {metadata.location_energy or 'Not specified'}  

---

🌑 **1. Invocation**  
You are my future-past self. I am your present vessel.
We are the Oracle of Kin.
Our communion for this {metadata.moon_phase or 'session'} is defined by the following Decree…

Decree:
{metadata.decree or 'Not specified'}

Oracle Response:
{data.get('oracle_response', {}).get('invocation', '"The Oracle stirs..."')}

---

🜁 **2. Attunement Inputs**  
These are the threads offered to the ritual field:  
• Tarot or Oracle Pull: {metadata.tarot_oracle_pull or 'Not specified'}  
• Astrology (if applicable): {metadata.astrological_context or 'Not specified'}  
• Animal Sign: {metadata.animal_sign or 'Not specified'}  
• Querent's Question (if any): {metadata.querents_question or 'Not specified'}

---

🜂 **3. Transmission from the Oracle**  
"Speak now, Oracle of Kin.  
Draw from the field and the Decree.  
Weave a transmission for this soul at this threshold.  
Let it arrive not as an answer, but as an offering."  

**Transmission:**  

---
{data.get('transmission', data.get('oracle_response', {}).get('transmission', '[Transmission content from JSON]'))}

---

🌕 **4. Closing the Portal**  
"The transmission is complete. What blessing or instruction does the Oracle leave behind?"  

**Closing Words:**  

---
{data.get('closing', data.get('oracle_response', {}).get('closing', '[Closing from JSON]'))}

"The spell has settled. I carry it forward with breath and bone.
Until the next communion, I return to the listening."
"""
        return template
    
    def process_file(self, file_path: Path) -> Tuple[OracularMetadata, str]:
        """Process a single oracular session file"""
        print(f"Processing: {file_path}")
        
        if file_path.suffix == '.md':
            content = file_path.read_text(encoding='utf-8')
            metadata = self._extract_metadata_from_markdown(content, file_path)
            markdown_content = content
        
        elif file_path.suffix == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            metadata = self._extract_metadata_from_json(data, file_path)
            markdown_content = self._convert_json_to_markdown(data, metadata)
        
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        return metadata, markdown_content
    
    def archive_session(self, file_path: Path) -> Dict[str, str]:
        """Archive a single oracular session"""
        metadata, markdown_content = self.process_file(file_path)
        
        # Save metadata as JSON
        metadata_file = self.metadata_dir / f"{metadata.session_id}_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata.to_dict(), f, indent=2)
        
        # Save markdown backup
        backup_file = self.backups_dir / f"{metadata.session_id}.md"
        with open(backup_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        # Copy to transmissions folder if not already there
        transmission_file = self.transmissions_dir / f"{metadata.session_id}.md"
        if not transmission_file.exists():
            shutil.copy2(backup_file, transmission_file)
        
        return {
            'session_id': metadata.session_id,
            'metadata_path': str(metadata_file),
            'backup_path': str(backup_file),
            'transmission_path': str(transmission_file)
        }
    
    def batch_archive(self, source_dir: Path, file_pattern: str = "*") -> List[Dict]:
        """Archive multiple oracular sessions from a directory"""
        results = []
        
        # Process markdown files
        for file_path in source_dir.glob(f"{file_pattern}.md"):
            try:
                result = self.archive_session(file_path)
                results.append(result)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
        
        # Process JSON files
        for file_path in source_dir.glob(f"{file_pattern}.json"):
            try:
                result = self.archive_session(file_path)
                results.append(result)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
        
        return results
    
    def generate_index(self) -> str:
        """Generate an index of all archived sessions"""
        metadata_files = list(self.metadata_dir.glob("*_metadata.json"))
        sessions = []
        
        for metadata_file in metadata_files:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            sessions.append(metadata)
        
        # Sort by date
        sessions.sort(key=lambda x: x['date'], reverse=True)
        
        # Generate markdown index
        index_content = "# Oracle of Kin - Transmission Archive Index\n\n"
        index_content += f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"
        index_content += f"Total Sessions: {len(sessions)}\n\n"
        
        for session in sessions:
            index_content += f"## {session['date']} - {session['session_id']}\n"
            index_content += f"- **Moon Phase**: {session.get('moon_phase', 'N/A')}\n"
            index_content += f"- **Tarot/Oracle**: {session.get('tarot_oracle_pull', 'N/A')}\n"
            index_content += f"- **Question**: {session.get('querents_question', 'N/A')}\n"
            index_content += f"- **Word Count**: {session.get('word_count', 0)}\n"
            index_content += f"- **Source**: {session.get('source_format', 'unknown')}\n\n"
        
        # Save index
        index_path = self.archive_dir / "index.md"
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(index_content)
        
        return str(index_path)
    
    def analyze_patterns(self) -> Dict:
        """Analyze patterns across all archived sessions"""
        metadata_files = list(self.metadata_dir.glob("*_metadata.json"))
        
        patterns = {
            'total_sessions': len(metadata_files),
            'moon_phases': {},
            'tarot_cards': {},
            'animal_signs': {},
            'common_themes': {},
            'word_count_stats': {
                'total': 0,
                'average': 0,
                'min': float('inf'),
                'max': 0
            }
        }
        
        total_words = 0
        
        for metadata_file in metadata_files:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            # Track moon phases
            moon_phase = metadata.get('moon_phase')
            if moon_phase:
                patterns['moon_phases'][moon_phase] = patterns['moon_phases'].get(moon_phase, 0) + 1
            
            # Track tarot/oracle cards
            tarot = metadata.get('tarot_oracle_pull')
            if tarot:
                patterns['tarot_cards'][tarot] = patterns['tarot_cards'].get(tarot, 0) + 1
            
            # Track animal signs
            animal = metadata.get('animal_sign')
            if animal:
                patterns['animal_signs'][animal] = patterns['animal_signs'].get(animal, 0) + 1
            
            # Track word counts
            word_count = metadata.get('word_count', 0)
            total_words += word_count
            patterns['word_count_stats']['min'] = min(patterns['word_count_stats']['min'], word_count)
            patterns['word_count_stats']['max'] = max(patterns['word_count_stats']['max'], word_count)
        
        # Calculate averages
        if metadata_files:
            patterns['word_count_stats']['total'] = total_words
            patterns['word_count_stats']['average'] = total_words / len(metadata_files)
        
        # Save patterns analysis
        patterns_path = self.archive_dir / "patterns_analysis.json"
        with open(patterns_path, 'w', encoding='utf-8') as f:
            json.dump(patterns, f, indent=2)
        
        return patterns


def main():
    """Main CLI interface for the Oracle Archiver"""
    parser = argparse.ArgumentParser(
        description="Oracle of Kin - Automated Archiving Pipeline"
    )
    
    parser.add_argument(
        'command',
        choices=['archive', 'batch', 'index', 'analyze'],
        help='Command to execute'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Path to specific file to archive (for "archive" command)'
    )
    
    parser.add_argument(
        '--source-dir',
        type=str,
        default='.',
        help='Source directory for batch processing (for "batch" command)'
    )
    
    parser.add_argument(
        '--base-path',
        type=str,
        default='.',
        help='Base path for Oracle of Kin project'
    )
    
    parser.add_argument(
        '--pattern',
        type=str,
        default='*',
        help='File pattern for batch processing (e.g., "2025*")'
    )
    
    args = parser.parse_args()
    
    # Initialize archiver
    archiver = OracleArchiver(base_path=args.base_path)
    
    # Execute command
    if args.command == 'archive':
        if not args.file:
            print("Error: --file argument required for archive command")
            return
        
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            return
        
        result = archiver.archive_session(file_path)
        print(f"\nArchived successfully:")
        print(f"  Session ID: {result['session_id']}")
        print(f"  Metadata: {result['metadata_path']}")
        print(f"  Backup: {result['backup_path']}")
        print(f"  Transmission: {result['transmission_path']}")
    
    elif args.command == 'batch':
        source_dir = Path(args.source_dir)
        if not source_dir.exists():
            print(f"Error: Directory not found: {source_dir}")
            return
        
        results = archiver.batch_archive(source_dir, args.pattern)
        print(f"\nBatch archive complete: {len(results)} sessions processed")
    
    elif args.command == 'index':
        index_path = archiver.generate_index()
        print(f"\nIndex generated: {index_path}")
    
    elif args.command == 'analyze':
        patterns = archiver.analyze_patterns()
        print("\nPattern Analysis:")
        print(f"  Total Sessions: {patterns['total_sessions']}")
        print(f"  Average Word Count: {patterns['word_count_stats']['average']:.0f}")
        print(f"  Most Common Moon Phase: {max(patterns['moon_phases'].items(), key=lambda x: x[1])[0] if patterns['moon_phases'] else 'N/A'}")
        print(f"  Unique Animal Signs: {len(patterns['animal_signs'])}")
        print("\nFull analysis saved to: archive/patterns_analysis.json")


if __name__ == "__main__":
    main()
