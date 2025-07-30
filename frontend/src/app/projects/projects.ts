import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms'; 
import { ProjectService } from '../services/project';
import { Project } from '../models/Project';

@Component({
  standalone: true,
  selector: 'app-projects',
  imports: [
    CommonModule,
    HttpClientModule,
    FormsModule 
  ],
  templateUrl: './projects.html',
  styleUrl: './projects.css',
  providers: [ProjectService]
})
export class Projects {
  projects: Project[] = [];
  filteredProjects: Project[] = []; 
  searchTerm: string = ''; 
  loading = false;
  error: string | null = null;

  recommendedDevs: any[] = [];
  selectedRepo: string | null = null;
  showPopup = false;

  selectedProject: any = null;
  showDetails = false;

  constructor(private projectService: ProjectService) {}

  ngOnInit(): void {
    this.loading = true;
    this.projectService.getProjects().subscribe({
      next: (data) => {
        this.projects = data;
        this.filteredProjects = data;
        this.loading = false;
      },
      error: (err) => {
        this.error = "Erreur lors du chargement des projets.";
        this.loading = false;
        console.error(err);
      }
    });
  }

  onSearchChange(): void {
    const term = this.searchTerm.toLowerCase().trim();
  
    this.filteredProjects = this.projects.filter(project =>
      project.repository.toLowerCase().includes(term) ||
      project.languages?.some(lang => lang.toLowerCase().includes(term)) ||
      project.frameworks?.some(fw => fw.toLowerCase().includes(term))
    );
  }
  

  openRecommendPopup(repository: string) {
    this.selectedRepo = repository;
    this.projectService.getRecommendedDevelopers(repository).subscribe({
      next: (data) => {
        this.recommendedDevs = data;
        this.showPopup = true;
      },
      error: (err) => {
        this.error = "Erreur lors du chargement des développeurs recommandés.";
        console.error(err);
      }
    });
  }

  closePopup() {
    this.showPopup = false;
    this.recommendedDevs = [];
  }

  viewDetails(repository: string) {
    this.projectService.getProjectDetails(repository).subscribe({
      next: (project) => {
        this.selectedProject = project;
        this.showDetails = true;
      },
      error: (err) => {
        console.error('Erreur chargement détails', err);
      }
    });
  }
  
  showFullSummary = false;

  closeDetails() {
    this.showFullSummary = false;
    this.selectedProject = null; 
  }

  projectToDelete: any = null;

  confirmDelete(project: any) {
    this.projectToDelete = project;
  }

  cancelDelete() {
    this.projectToDelete = null;
  }

  deleteProject(project: any): void {
    this.projectService.deleteProject(project.repository).subscribe({
      next: () => {
        // Retirer le projet supprimé de la liste
        this.projects = this.projects.filter(p => p.repository !== project.repository);
        this.filteredProjects = this.filteredProjects.filter(p => p.repository !== project.repository);
  
        // Fermer le popup de confirmation
        this.projectToDelete = null;
      },
      error: (err) => {
        console.error('Erreur de suppression', err);
      }
    });
  }
  
  showAddPopup = false;
  newProject = {
    repository: '',
    languagesInput: '',
    frameworksInput: ''
  };
  selectedReadmeFile: File | null = null;

  // Ouvre popup ajout
  openAddPopup() {
    this.showAddPopup = true;
    this.newProject = { repository: '', languagesInput: '', frameworksInput: '' };
    this.selectedReadmeFile = null;
  }

  // Ferme popup ajout
  closeAddPopup() {
    this.showAddPopup = false;
  }

  // Récupérer le fichier sélectionné
  onFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      this.selectedReadmeFile = input.files[0];
    }
  }

  // Soumettre le nouveau projet
  submitNewProject() {
    if (!this.newProject.repository || !this.selectedReadmeFile) return;

    // Convertir les champs texte en array (split par virgule)
    const languages = this.newProject.languagesInput
      ? this.newProject.languagesInput.split(',').map(s => s.trim()).filter(s => s.length > 0)
      : [];
    const frameworks = this.newProject.frameworksInput
      ? this.newProject.frameworksInput.split(',').map(s => s.trim()).filter(s => s.length > 0)
      : [];

    const formData = new FormData();
    formData.append('repository', this.newProject.repository);
    languages.forEach(lang => formData.append('languages', lang));
    frameworks.forEach(fw => formData.append('frameworks', fw));
    formData.append('readme', this.selectedReadmeFile);

    this.projectService.addProject(formData).subscribe({
      next: (res) => {
        console.log('project added successfully')
        this.showAddPopup = false;
        this.ngOnInit();
      },
      error: (err) => {
        console.error(err);
      }
    });
  }
  
}
