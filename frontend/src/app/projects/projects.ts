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
  frameworksInput: '',
  featuresInput: '',
  summary: ''
};

// Ouvre popup ajout
openAddPopup() {
  this.showAddPopup = true;
  this.newProject = {
    repository: '',
    languagesInput: '',
    frameworksInput: '',
    featuresInput: '',
    summary: ''
  };
}

// Ferme popup ajout
closeAddPopup() {
  this.showAddPopup = false;
}

// Soumettre le nouveau projet sans README
submitNewProject() {
  if (!this.newProject.repository) return;

  const languages = this.newProject.languagesInput
    ? this.newProject.languagesInput.split(',').map(s => s.trim()).filter(Boolean)
    : [];

  const frameworks = this.newProject.frameworksInput
    ? this.newProject.frameworksInput.split(',').map(s => s.trim()).filter(Boolean)
    : [];

  const features = this.newProject.featuresInput
    ? this.newProject.featuresInput.split(',').map(s => s.trim()).filter(Boolean)
    : [];

  const body = {
    repository: this.newProject.repository,
    languages,
    frameworks,
    features,
    summary: this.newProject.summary
  };

  this.projectService.addProject(body).subscribe({
    next: (res) => {
      console.log('Projet ajouté');
      this.showAddPopup = false;
      this.ngOnInit();
    },
    error: (err) => {
      console.error('Erreur ajout', err);
    }
  });
}
isEditing: boolean = false;
editProject: any = {};

startEdit() {
  this.isEditing = true;
  this.editProject = {
    languagesInput: this.selectedProject.languages?.join(', ') || '',
    frameworksInput: this.selectedProject.frameworks?.join(', ') || '',
    featuresInput: this.selectedProject.features?.join(', ') || '',
    domainInput: this.selectedProject.domain?.join(', ') || '',
    summary: this.selectedProject.summary || ''
  };
}

saveEdit() {
  const updatedData = {
    languages: this.editProject.languagesInput.split(',').map((s: string) => s.trim()).filter(Boolean),
    frameworks: this.editProject.frameworksInput.split(',').map((s: string) => s.trim()).filter(Boolean),
    features: this.editProject.featuresInput.split(',').map((s: string) => s.trim()).filter(Boolean),
    domain: this.editProject.domainInput.split(',').map((s: string) => s.trim()).filter(Boolean),
    summary: this.editProject.summary
  };

  this.projectService.updateProject(this.selectedProject.repository, updatedData).subscribe({
    next: () => {
      Object.assign(this.selectedProject, updatedData);
      this.isEditing = false;
    },
    error: (err) => {
      console.error('Erreur modification', err);
    }
  });
}

cancelEdit() {
  this.isEditing = false;
}


}