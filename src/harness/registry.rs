use std::sync::{Arc, Mutex};

use std::collections::BTreeMap;

use crate::errors::{Error, Result};
use crate::harness::agent::INSTRUCTIONS_KEY;
use crate::harness::compaction;
use crate::harness::erased::erase;
use crate::harness::generation;
use crate::harness::scheduler::ErasedTask;
use crate::harness::tool;
use crate::harness::types::{Extension, PromptSection, ToolRegistration};

pub type Listener = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub struct RegistrySnapshot {
    extensions: Vec<Arc<Extension>>,
    tasks: BTreeMap<String, Arc<dyn ErasedTask>>,
}

pub fn builtin_tasks() -> Vec<Arc<dyn ErasedTask>> {
    vec![
        erase(generation::definition()),
        erase(tool::tool_task()),
        erase(compaction::definition()),
    ]
}

impl RegistrySnapshot {
    fn build(extensions: Vec<Arc<Extension>>) -> Result<Self> {
        let mut tasks = BTreeMap::new();
        for task in builtin_tasks() {
            tasks.insert(task.name().to_owned(), task);
        }
        for extension in &extensions {
            for task in &extension.tasks {
                if tasks.contains_key(task.name()) {
                    return Err(Error::Invalid(format!(
                        "Task {} of extension {} is already installed",
                        task.name(),
                        extension.name
                    )));
                }
                tasks.insert(task.name().to_owned(), Arc::clone(task));
            }
        }
        Ok(Self { extensions, tasks })
    }

    pub fn task(&self, name: &str) -> Option<&Arc<dyn ErasedTask>> {
        self.tasks.get(name)
    }

    pub fn tasks(&self) -> impl Iterator<Item = &Arc<dyn ErasedTask>> {
        self.tasks.values()
    }

    pub fn installed(&self) -> &[Arc<Extension>] {
        &self.extensions
    }

    pub fn extension(&self, name: &str) -> Option<&Arc<Extension>> {
        self.extensions
            .iter()
            .find(|extension| extension.name == name)
    }

    pub fn tools(&self) -> impl Iterator<Item = (&Arc<Extension>, &ToolRegistration)> {
        self.extensions
            .iter()
            .flat_map(|extension| extension.tools.iter().map(move |tool| (extension, tool)))
    }

    pub fn sections(&self) -> impl Iterator<Item = (&Arc<Extension>, &PromptSection)> {
        self.extensions.iter().flat_map(|extension| {
            extension
                .sections
                .iter()
                .map(move |section| (extension, section))
        })
    }
}

#[derive(Default)]
struct Current {
    snapshot: Arc<RegistrySnapshot>,
    listeners: Vec<(u64, Listener)>,
    next_listener: u64,
}

#[derive(Default)]
pub struct Registry(Mutex<Current>);

impl Registry {
    pub fn snapshot(&self) -> Arc<RegistrySnapshot> {
        self.with(|current| Arc::clone(&current.snapshot))
    }

    pub fn subscribe(&self, listener: Listener) -> u64 {
        self.with(|current| {
            let id = current.next_listener;
            current.next_listener += 1;
            current.listeners.push((id, listener));
            id
        })
    }

    pub fn unsubscribe(&self, id: u64) {
        self.with(|current| current.listeners.retain(|(listener, _)| *listener != id));
    }

    pub fn install(&self, extension: Extension) -> Result<()> {
        validate_extension(&extension)?;
        let next = {
            let current = self.snapshot();
            let mut extensions = current.extensions.clone();
            match extensions
                .iter()
                .position(|installed| installed.name == extension.name)
            {
                Some(index) => extensions[index] = Arc::new(extension),
                None => extensions.push(Arc::new(extension)),
            }
            extensions
        };
        self.publish(RegistrySnapshot::build(next)?);
        Ok(())
    }

    pub fn uninstall(&self, name: &str) -> Result<()> {
        let current = self.snapshot();
        if current.extension(name).is_none() {
            return Ok(());
        }
        let next = current
            .extensions
            .iter()
            .filter(|installed| installed.name != name)
            .cloned()
            .collect();
        self.publish(RegistrySnapshot::build(next)?);
        Ok(())
    }

    fn publish(&self, snapshot: RegistrySnapshot) {
        let listeners = self.with(|current| {
            current.snapshot = Arc::new(snapshot);
            current
                .listeners
                .iter()
                .map(|(_, listener)| Arc::clone(listener))
                .collect::<Vec<_>>()
        });
        for listener in listeners {
            listener();
        }
    }

    fn with<T>(&self, use_current: impl FnOnce(&mut Current) -> T) -> T {
        let mut guard = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        use_current(&mut guard)
    }
}

pub fn valid_section_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some('a'..='z'))
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_' | '-'))
}

fn validate_extension(extension: &Extension) -> Result<()> {
    let mut tools: Vec<&str> = Vec::new();
    for tool in &extension.tools {
        let name = tool.tool.name.as_str();
        if tools.contains(&name) {
            return Err(Error::Invalid(format!(
                "Extension {} has two tools named {name}",
                extension.name
            )));
        }
        tools.push(name);
    }
    let mut keys: Vec<&str> = Vec::new();
    for section in &extension.sections {
        let key = section.key.as_str();
        if !valid_section_key(key) {
            return Err(Error::Invalid(format!(
                "Section key {key:?} must match ^[a-z][a-z0-9_-]*$"
            )));
        }
        if key == INSTRUCTIONS_KEY {
            return Err(Error::Invalid(format!(
                "Section key {key} is reserved for the agent's instructions"
            )));
        }
        if keys.contains(&key) {
            return Err(Error::Invalid(format!(
                "Extension {} has two sections with key {key}",
                extension.name
            )));
        }
        keys.push(key);
    }
    Ok(())
}
